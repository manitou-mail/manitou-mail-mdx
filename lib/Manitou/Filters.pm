# Copyright (C) 2004-2015 Daniel Verite

# This file is part of Manitou-Mail (see http://www.manitou-mail.org)

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.

# Filtering code for manitou-mdx

use strict;

package Manitou::Filters;

use Data::Dumper;
use MIME::Head;
use MIME::Parser;
use MIME::Words qw(:all);
use Manitou::Encoding;
use Manitou::Config qw(getconf_bool);
use DBI;

my $filter_exprs;

# ctxt->evp: position in expression during left->right traversal
# ctxt->expr: current expression
# ctxt->len: equal to length(ctxt->expr)
# ctxt->npar: current level of parenthesis
# ctxt->errstr: error string
# ctxt->evstack: list of operand values handled as a stack
# ctxt->mime_obj: mime object for current mail
# ctxt->mail_id: mail_id of current mail if available
# ctxt->call_stack: stack of condition names being evaluated (used to prevent infinite recursion)

my $PRI_DOT=10;
my $PRI_AND=24;
my $PRI_OR=22;
my $PRI_UNARY_NOT=30;
my $PRI_CMP=40;
my $PRI_CONTAINS=40;
my $PRI_REGEXP=40;

my $TYPE_STRING=1;
my $TYPE_FUNC=2;
my $TYPE_NUMBER=3;
my $TYPE_SUBEXPR=4;
my $TYPE_PSTRING=5;
my $TYPE_MAILID=6;

my %eval_binary_ops =
  (
   "and" => {"func" => sub { return $_[0] && $_[1]; },
	     "pri" => $PRI_AND},
   "or"  => {"func" => sub { return $_[0] || $_[1]; },
	     "pri" => $PRI_OR},
   "contains"  => {"func" => sub { return index($_[0],$_[1])>=0; },
	     "pri" => $PRI_CONTAINS},
   # alias for 'contains'
   "contain"  => {"func" => sub { $_[0] =~ /\Q$_[1]\E/i; },
	     "pri" => $PRI_CONTAINS},
   "equals"  => {"func" => sub { $_[0] eq $_[1]; },
	     "pri" => $PRI_CMP},
   "eq"  => {"func" => sub { $_[0] eq $_[1]; },
	     "pri" => $PRI_CMP},
   "ne"  => {"func" => sub { $_[0] ne $_[1]; },
	     "pri" => $PRI_CMP},
   "is"  => {"func" => sub { uc($_[0]) eq uc($_[1]); },
	     "pri" => $PRI_CMP},
   "isnot"  => {"func" => sub { uc($_[0]) ne uc($_[1]); },
	     "pri" => $PRI_CMP},
   "regmatches"  => {"func" => sub { $_[0] =~ /$_[1]/ },
	     "pri" => $PRI_REGEXP}
  );

my %eval_unary_ops =
  (
   "not" => {"func" => sub { return !$_[0]; },
	     "pri" => $PRI_UNARY_NOT}
  );


sub get_parent {
  my ($mailobj,$dbh) = @_;
  my $in_reply_to=$mailobj->head->get('In-Reply-To');
  my $sth=$dbh->prepare("SELECT mail_id FROM mail WHERE message_id=?");
  if ($in_reply_to =~ /.*\<(.*)\>/) {
    $in_reply_to=$1;
    $sth->execute($in_reply_to);
    my @r=$sth->fetchrow_array;
    return $r[0] if (@r);
  }

  my $other_mails=$mailobj->head->get('References');
  my @thread_msgs=split / /, $other_mails;
  for my $m (reverse(@thread_msgs)) {
    $m=$1 if ($m =~ /.*\<(.*)\>/);
    $sth->execute($m);
    my @r=$sth->fetchrow_array;
    return $r[0] if (@r);
  }
  undef;
}

# sub func_parent {
#   my ($ctxt)=@_;
#   my $mail_id=$ctxt->{mail_id};
#   if ($mail_id) {
#     my $sth=$ctxt->{dbh}->prepare("SELECT in_reply_to FROM mail WHERE mail_id=?");
#     $sth->execute($mail_id);
#     my @r=$sth->fetchrow_array;
#     return $r[0];		# may be undef
#   }
#   elsif ($ctxt->{mime_obj}) {
#     return get_parent($ctxt->{mime_obj}, $ctxt->{dbh});
#   }
#   undef;
# }

sub list_addresses {
  my ($o, $field)=@_;
  my @a;
  eval {
    @a = Mail::Address->parse(Manitou::Encoding::header_decode($o->head->get($field)));
  };
  return join ',', map { $_->address() } @a;
}

sub func_mimeobj_size {
  my $top=shift;
  my $size=0;
  if ($top->is_multipart) {
    foreach my $p ($top->parts) {
      $size += func_mimeobj_size($p);
    }
  }
  else {
    if (defined $top->bodyhandle) {
      $size = length($top->body);
    }
  }
  return $size;
}

sub date_field {
  my $ctxt=shift;
  my $field=shift;
  my %fmts=("hour"=>"%H", "minute"=>"%M", "second"=>"%S",
	    "day"=>"%d", "month"=>"%m", "year"=>"%Y", "weekday"=>"%w",
	    "date"=>"%Y-%m-%d", time=>"%H:%M:%S");
  my $f = $fmts{lc($field)};
  if (!defined $f) {
    $ctxt->{errstr} = "Invalid date/time field '$field'";
    return undef;
  }
  POSIX::strftime($f, @_);
}

my %eval_funcs =
  (
   "age" =>
   {"func" => sub {
      my ($ctxt,$u)=@_;
      my %units=("days"=>86400, "hours"=>3600, "minutes"=>60);
      if (!exists $units{$u}) {
	$ctxt->{errstr} = "age() argument must be \"days\", \"hours\", or \"minutes\"";
	return undef;
      }
      my $h=$ctxt->{mime_obj}->head->get("Date");
      return undef if (!defined $h);
      my $t = Manitou::MailFormat::convert_sender_date_to_timestamp($h);
      return undef if (!defined $t);
      return (time()-$t)/$units{$u};
    },
    "args" => 1,
    "return_type" => $TYPE_NUMBER
   },

   "body" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      return $o->bodyhandle->as_string if ($o->bodyhandle);
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "cc" =>
   {"func" => sub {
      return list_addresses($_[0]->{mime_obj}, "cc");
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "condition" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my ($v,$t) = eval_subexpr($ctxt, $_[1]);
      return ($v, $t);
    },
    "args" => 1,
    "return_type" => $TYPE_NUMBER
   },


   "date" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $h=$ctxt->{mime_obj}->head->get("Date");
      return undef if (!defined $h);
      my $t = Manitou::MailFormat::convert_sender_date_to_timestamp($h);
      return undef if (!defined $t);
      date_field($ctxt, $_[1], localtime($t));
    },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "date_utc" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $h=$ctxt->{mime_obj}->head->get("Date");
      return undef if (!defined $h);
      my $t = Manitou::MailFormat::convert_sender_date_to_timestamp($h);
      return undef if (!defined $t);
      date_field($ctxt, $_[1], gmtime($t));
    },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "from" =>
   {"func" => sub {
      return list_addresses($_[0]->{mime_obj}, "from");
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "header" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      my $v = $o->head->get($_[1]);
      chomp $v;
      return Manitou::Encoding::header_decode($v);
    },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "headers" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      if (!exists $ctxt->{cache}->{headers}) {
	$ctxt->{cache}->{headers}=Manitou::Encoding::header_decode($o->head->as_string);
      }
      return $ctxt->{cache}->{headers};
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "identity" =>
   {"func" => sub {
      return $_[0]->{mailbox_address};
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "rawheader" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      my $v = $o->head->get($_[1]);
      chomp $v;
      return $v;
    },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "rawheaders" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      return $o->head->as_string;
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "rawsize" =>
   {"func" => sub {
      my $ctxt=$_[0];
      return $ctxt->{mail_ctxt}->{filesize};
    },
    "args"=>0,
    "return_type"=>$TYPE_NUMBER,
   },

   "recipients" =>   # Returns all recipients (To, Cc, Bcc) separated by commas
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      if (exists $ctxt->{cache}->{recipients}) {
	return $ctxt->{cache}->{recipients};
      }
      my $r;
      for my $h ("To", "Cc", "Bcc") {
	my $v = Manitou::Encoding::header_decode($o->head->get($h));
	$r = defined $r ? "$r,$v" : $v;
      }
      return $ctxt->{cache}->{recipients}=$r;
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "now" =>
   {"func" => sub { date_field($_[0], $_[1], localtime($_[0]->{start_time})); },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "now_utc" =>
   {"func" => sub { date_field($_[0], $_[1], gmtime($_[0]->{start_time})); },
    "args" => 1,
    "return_type" => $TYPE_STRING
   },

   "subject" =>
   {"func" => sub {
      my $ctxt=$_[0];
      my $o=$ctxt->{mime_obj};
      my $v = $o->head->get("subject");
      chomp $v;
      Manitou::Encoding::header_decode($v);
    },
    "args" => 0,
    "return_type" => $TYPE_STRING
   },

   "to" =>
   {"func" => sub { list_addresses($_[0]->{mime_obj}, "to"); },
    "args" => 0,
    "return_type" => $TYPE_STRING
   }

  );

sub nxchar {
  my ($ctxt)=@_;
  # test against the length to avoid the warning emitted by substr()
  # when trying to get a character outside of the string
  if ($ctxt->{evp} < $ctxt->{len}) {
    return substr($ctxt->{expr}, $ctxt->{evp}, 1);
  }
  return undef;
}

sub nxchar1 {
  my ($ctxt)=@_;
  if ($ctxt->{evp}+1 < $ctxt->{len}) {
    return substr($ctxt->{expr}, $ctxt->{evp}+1, 1);
  }
  return undef;
}

sub skip_blanks {
  my ($ctxt)=@_;
  my $c;
  my $p=$ctxt->{evp};
  do {
    $c=substr($ctxt->{expr}, $p, 1);
    $p++;
  } while ($c eq " " || $c eq "\t" || $c eq "\n" || $c eq "\r");
  $ctxt->{evp}=$p-1;
}

sub getnum {
  my ($ctxt)=@_;
  my $v=0;
  my $end=0;
  my $c;
  my $p=$ctxt->{evp};
  do {
    $c=substr($ctxt->{expr}, $p++, 1);
    if ($c =~ /[0-9]/) {
      $v = $v*10+$c;
    }
    else {
	# k,m or g following a number means kbytes,mbytes or gbytes
      my $m=lc($c);
      if ($m eq "k") { $v *= 1024; }
      elsif ($m eq "m") { $v *= 1024*1024; }
      elsif ($m eq "g") { $v *= 1024*1024*1024 }
      else { $p--; }
      $end=1;
    }
  } while (!$end);
  $ctxt->{evp}=$p;
  push @{$ctxt->{evstack}}, [$v, $TYPE_NUMBER];
}

sub eval_subexpr {
  my ($ctxt,$exprname)=@_;
  if (defined($ctxt->{cache_subexpr}->{$exprname})) {
    return @{$ctxt->{cache_subexpr}->{$exprname}};
  }
  elsif (defined($filter_exprs->{$exprname})) {
    # Prevent infinite recursion
    if (grep { $_ eq $exprname } @{$ctxt->{call_stack}}) {
      $ctxt->{errstr}="Infinite recursion requested";
      return undef;
    }

    my $evstr=$filter_exprs->{$exprname}->{expr};
    my %subctxt;
    $subctxt{mime_obj}=$ctxt->{mime_obj};
    $subctxt{evp}=0;
    $subctxt{cache} = $ctxt->{cache};
    $subctxt{expr}=$evstr;
    $subctxt{len}=length($evstr);
    $subctxt{call_stack} = $ctxt->{call_stack};
    push @{$subctxt{call_stack}}, $exprname;
    if (eval_expr(0, \%subctxt)) {
      my ($pv2) = pop(@{$subctxt{evstack}});
      $ctxt->{cache_subexpr}->{$exprname}=@{$pv2};
      return @{$pv2};
    }
    else {
      $ctxt->{errstr} = "Error $exprname: ". $subctxt{errstr}. " at character ". $subctxt{evp}+1;
      return undef;
    }
  }
  else {
    $ctxt->{errstr}="Unknown function \"$exprname\"";
    return undef;
  }
}

# Returns the symbol in lower case
sub getsym {
  my ($ctxt)=@_;
  my $v;
  my $end=0;
  my $c;
  my $p=$ctxt->{evp};
  do {
    $c=lc(substr($ctxt->{expr}, $p, 1));
    $p++;
    if ($c =~ /[a-z_0-9]/) { $v .= $c; }
    else { $end=1; }
  } while (!$end);
  $ctxt->{evp}=$p-1;
  return $v;
}

sub get_op_name {
  my ($ctxt)=@_;
  my $v;
  my $end=0;
  my $c;
  my $p=$ctxt->{evp};
  do {
    $c=substr($ctxt->{expr}, $p++, 1);
    if ($c =~ /[A-Za-z_0-9]/) { $v .= lc($c); }
    else { $end=1; }
  } while (!$end);
  $ctxt->{evp}=$p-1;
  return $v;
}

# parse a string that should be ended by the $endc character
# interpret backslash-style quoting if ($quote)
sub eval_string {
  my ($ctxt, $endc, $quote)=@_;
  my $end=0;
  my $c;
  my $v;
  my $p=$ctxt->{evp};
  my $l=$ctxt->{len};
  while (!$end) {
    if ($p==$l) {
      $ctxt->{errstr}="Premature end of string";
      $ctxt->{evp}=$p-1;
      return undef;
    }
    else {
      $c=substr($ctxt->{expr}, $p++, 1);
      if ($quote && $c eq "\\") {
	if ($p<$l) {
	  $c = substr($ctxt->{expr}, $p++, 1);
	  $v .= $c;
	}
	else {
	  next; # and error
	}
      }
      elsif ($c eq $endc) {
	$end=1;
      }
      else {
	$v .= $c;
      }
    }
  }
  $ctxt->{evp}=$p;
  push @{$ctxt->{evstack}}, [$v,$TYPE_STRING];
  1;
}

sub process_binary_op {
  my ($ctxt, $c, $pri)=@_;
  $ctxt->{evp}++;
  if (eval_expr($pri, $ctxt)) {
    my $pv2=pop(@{$ctxt->{evstack}});
    my $pv1=pop(@{$ctxt->{evstack}});
    my ($v2,$t2)=@{$pv2};
    my ($v1,$t1)=@{$pv1};
    my $res;
    if ($c eq "=") {
      $res = (uc($v1) eq uc($v2))?1:0;
    }
    elsif ($c eq "==") {
      $res = ($v1 == $v2)?1:0;
    }
    elsif ($c eq "=~") {
      $res = ($v1 =~ /$v2/i)?1:0;
    }
    elsif ($c eq "!~") {
      $res = ($v1 !~ /$v2/i)?1:0;
    }
    elsif ($c eq "<") {
      $res = ($v1 < $v2)?1:0;
    }
    elsif ($c eq ">") {
      $res = ($v1 > $v2)?1:0;
    }
    elsif ($c eq ">=") {
      $res = ($v1 >= $v2)?1:0;
    }
    elsif ($c eq "<=") {
      $res = ($v1 <= $v2)?1:0;
    }
    elsif ($c eq "!=") {
      $res = ($v1 != $v2)?1:0;
    }
    push(@{$ctxt->{evstack}}, [$res, $TYPE_NUMBER]);
  }
}

sub eval_expr {
  my ($level, $ctxt)=@_;
  my $endexpr=0;

  skip_blanks($ctxt);

  #### operand or unary operator followed by operand ####
  my $c=nxchar($ctxt);
  if ($c eq "(") {
    $ctxt->{npar}++;
    $ctxt->{evp}++;
    if (eval_expr(0, $ctxt) && nxchar($ctxt) ne ")") {
      $ctxt->{errstr}="Unmatched parenthesis";
    }
    $ctxt->{npar}--;
    $ctxt->{evp}++;		# eat ')'
  }
  elsif ($c eq ")") {
    # We're accepting parentheses around an empty content, but only
    # in the context of evaluating function arguments
    my $last_elt=@{$ctxt->{evstack}};
    if ($last_elt>0 && $ctxt->{npar}>0) {
      my $pv=@{$ctxt->{evstack}}[$last_elt-1];
      if (@{$pv}[1] == $TYPE_FUNC) {
	$endexpr=1;
      }
    }
    if (!$endexpr) {
      $ctxt->{errstr}="Unmatched closing parenthesis";
    }
  }
  elsif ($c eq '"' || $c eq "'") {
    $ctxt->{evp}++;
    return undef if (!eval_string($ctxt, $c, 0));
  }
  elsif ($c eq '\\') {
    my $c1=nxchar1($ctxt);
    if ($c1 eq '"' || $c1 eq "'") {
      $ctxt->{evp}+=2;
      return undef if (!eval_string($ctxt, $c1, 1));
    }
    else {
      $ctxt->{errstr}="Unexpected character '\\'";
    }
  }
  elsif ($c eq "!") {		# logical not
    if ($level <= $PRI_UNARY_NOT) {
      $ctxt->{evp}++;
      if (eval_expr($PRI_UNARY_NOT, $ctxt)) {
	my $pv=pop(@{$ctxt->{evstack}});
	my ($v,$t)=@{$pv};
	push(@{$ctxt->{evstack}}, [!$v,$t]);
      }
    }
    else { $endexpr=1; }
  }
  elsif ($c =~ /^[0-9]$/) {
    getnum($ctxt);
  }
  elsif (lc($c) =~ /^[a-z_]$/) {
    my $startp=$ctxt->{evp};
    my $sym=getsym($ctxt);
    if (defined $eval_unary_ops{$sym}) {
      my $f=$eval_unary_ops{$sym};
      if ($level <= $f->{pri}) {
	if (eval_expr($f->{pri}, $ctxt)) {
	  my $pv=pop(@{$ctxt->{evstack}});
	  my ($v,$t)=@{$pv};
	  my $fn=$f->{func};
	  push(@{$ctxt->{evstack}}, [&$fn($v),$t]);
	}
      }
      else {
	$ctxt->{evp}=$startp;
	$endexpr=1;
      }
    }
    else {
      my $pfunc = $eval_funcs{$sym};
      if (defined $pfunc) {
	skip_blanks($ctxt);
	if (nxchar($ctxt) eq '(') {
	  $ctxt->{evp}++;
	  $ctxt->{npar}++;
	  skip_blanks($ctxt);
	  if (nxchar($ctxt) eq ')') {
	    $ctxt->{evp}++;
	    $ctxt->{npar}--;
	    if ($pfunc->{args}==0) {
	      my $fn = $pfunc->{func};
	      my $v = &$fn($ctxt);
	      push(@{$ctxt->{evstack}}, [$v,$pfunc->{return_type}]);
	    }
	    else {
	      $ctxt->{errstr} = "Missing argument to function '$sym'";
	    }
	  }
	  else {
	    if ($pfunc->{args}==0) {
	      $ctxt->{errstr} = "The function '$sym' does not accept any argument";
	    }
	    else {
	      my $stack_depth = @{$ctxt->{evstack}};
	      if (eval_expr(0, $ctxt)) {
		if (nxchar($ctxt) ne ')') {
		  $ctxt->{errstr} = "Unmatched parenthesis";
		}
		else {
		  $ctxt->{npar}--;
		  $ctxt->{evp}++;
		  my ($v_arg,$t);
		  if ($stack_depth != @{$ctxt->{evstack}}) {
		    my $pv = pop @{$ctxt->{evstack}};
		    ($v_arg,$t) = @{$pv};
		  }
		  my $fn=$pfunc->{func};
		  my $v=&$fn($ctxt,$v_arg,$t);
		  push @{$ctxt->{evstack}}, [$v, $pfunc->{return_type}];
		}
	      }
	      else {
		$endexpr=1;
	      }
	    }
	  }
	}
	else {
	  # no open parenthesis
	  if ($pfunc->{args}==0) {
	    my $fn=$pfunc->{func};
	    push @{$ctxt->{evstack}}, [&$fn($ctxt), $pfunc->{return_type}];
	  }
	  else {
	    $ctxt->{errstr}="Open parenthesis expected after function requiring arguments";
	  }
	}
      }
      else {
	# sub-expr, evaluate now
	my ($v,$t)=eval_subexpr($ctxt, $sym);
	if (!defined($v)) {
	  $ctxt->{evp}=$startp;		# step back to the error
	  return undef;
	}
	push @{$ctxt->{evstack}}, [$v,$t] ;
      }
    }
  }
  else {
    # default
    $ctxt->{errstr} = "Parse error";
  }
  return undef if ($ctxt->{errstr});

  #### binary operator and 2nd operand ####
  while (!$endexpr && !defined($ctxt->{errstr})) {
    if ($ctxt->{evp} >= length($ctxt->{expr})) {
      $endexpr=1;
      last;
    }
    skip_blanks($ctxt);
    $c=nxchar($ctxt);
    if ($c eq ")") {
      if ($ctxt->{npar}==0) {
	$ctxt->{errstr}="Unmatched closing parenthesis";
      }
      $endexpr=1;
    }
    elsif ($c =~ /[A-Za-z]/) {
      my $p=$ctxt->{evp};
      my $o=get_op_name($ctxt);
      my $f=$eval_binary_ops{$o};
      if (defined($f)) {
#	print Dumper($f);
	if ($level <= $f->{pri}) {
	  if (eval_expr($f->{pri}, $ctxt)) {
	    my $pv2=pop(@{$ctxt->{evstack}});
	    my $pv1=pop(@{$ctxt->{evstack}});
	    my ($v2,$t2)=@{$pv2};
	    my ($v1,$t1)=@{$pv1};
	    my $fn=$f->{func};
	    my $v=&$fn($v1,$v2);
	    push(@{$ctxt->{evstack}}, [$v, $TYPE_NUMBER]);
	  }
	}
	else {
	  $ctxt->{evp}=$p;
	  $endexpr=1;
	}
      }
      else {
	$ctxt->{evp}=$p;
	$ctxt->{errstr} = "Unknown operator";
      }
    }
    elsif ($c eq "=") {
      my $c1=nxchar1($ctxt);
      if ($c1 eq "~") {
	if ($level <= $PRI_REGEXP) {
	  $ctxt->{evp}++;
	  process_binary_op($ctxt, "=~", $PRI_REGEXP);
	}
	else { $endexpr=1; }
      }
      else {
	if ($level <= $PRI_CMP) {
	  process_binary_op($ctxt, "=", $PRI_CMP);
	}
	else { $endexpr=1; }
      }
    }
    elsif ($c eq "!") {
      my $c1=nxchar1($ctxt);
      if ($c1 eq "~") {
	if ($level <= $PRI_REGEXP) {
	  $ctxt->{evp}++;
	  process_binary_op($ctxt, "!~", $PRI_REGEXP);
	}
	else { $endexpr=1; }
      }
      elsif ($c1 eq "=") {
	if ($level <= $PRI_CMP) {
	  $ctxt->{evp}++;
	  process_binary_op($ctxt, "!=", $PRI_CMP);
	}
	else { $endexpr=1; }	
      }
      else {
	$ctxt->{errstr} = "Unexpected operator: !";
	$endexpr=1;
      }
    }
    elsif ($c eq "<") {
      if (nxchar1($ctxt) eq "=") {
	if ($level <= $PRI_CMP) {
	  $ctxt->{evp}++;
	  process_binary_op($ctxt, "<=", $PRI_CMP);
	}
	else { $endexpr=1; }
      }
      else {
	if ($level <= $PRI_CMP) {
	  process_binary_op($ctxt, "<", $PRI_CMP);
	}
	else { $endexpr=1; }
      }
    }
    elsif ($c eq ">") {
      if (nxchar1($ctxt) eq "=") {
	if ($level <= $PRI_CMP) {
	  $ctxt->{evp}++;
	  process_binary_op($ctxt, ">=", $PRI_CMP);
	}
	else { $endexpr=1; }
      }
      else {
	if ($level <= $PRI_CMP) {
	  process_binary_op($ctxt, ">", $PRI_CMP);
	}
	else { $endexpr=1; }
      }
    }
    elsif ($c eq "(") {
      # functions
      my $last_elt=@{$ctxt->{evstack}};
      my $pv=@{$ctxt->{evstack}}[$last_elt-1];
      if (@{$pv}[1] == $TYPE_FUNC) {
	$ctxt->{evp}++;
	$ctxt->{npar}++;
	# zero or one-arg only functions until now
	if (eval_expr(0, $ctxt)) {
	  $c=nxchar($ctxt);
	  if ($c ne ")") {
	    $ctxt->{errstr}="Closing parenthesis expected";
	    $endexpr=1;
	  }
	  else {
	    $ctxt->{npar}--;
	    $ctxt->{evp}++;
	    my ($v,$t);
	    if ($last_elt != @{$ctxt->{evstack}}) {
	      # if there is an argument (the stack is deeper than before evaluating
	      # arguments), then pop it
	      my $pv=pop(@{$ctxt->{evstack}});
	      ($v,$t)=@{$pv};
	    }
	    $pv=pop(@{$ctxt->{evstack}});
	    my ($func_name,$tfn)=@{$pv};
	    my $f=$eval_funcs{$func_name};
	    if (defined($f)) {
	      my $fn=$f->{func};
	      $v=&$fn($ctxt,$v,$t);
	      # functions are all prototyped func(string) at the moment
	      push(@{$ctxt->{evstack}}, [$v, $f->{return_type}]);
	    }
	    else {
	      $ctxt->{errstr} = "Function not found: $func_name";
	    }
	  }
	}
      }
      else {
	$ctxt->{errstr} = "Unexpected open parenthesis";
      }
    }
    else {
      $ctxt->{errstr} = "Unexpected operator";
    }
  }

  return undef if (defined($ctxt->{errstr}));
  1;
}

sub process_filter_mimeobj {
  my ($filter_expr, $top, $res, $mail_ctxt, $dbh, $exprs, $cond_name)=@_;
  my %h;
  $filter_exprs=$exprs;
  $h{mime_obj} = $top;
  $h{evp}=0;
  $h{errstr}=undef;
  $h{npar}=0;
  $h{expr}=$filter_expr;
  $h{len}=length($filter_expr);
  push @{$h{call_stack}}, $cond_name;
  $h{mail_ctxt} = $mail_ctxt; # a hash ref
  $h{mail_id} = $mail_ctxt->{mail_id};
  $h{start_time}=time();
  @{$h{evstack}}=();
  if (eval_expr(0, \%h)) {
    my ($pv2) = pop(@{$h{evstack}});
    my ($v2,$t2)=@{$pv2};
    if (!$v2) {
      $v2=0;
    }
    $$res=$v2;
  }
  else {
    $$res=undef;
    print STDERR "Error $filter_expr: ", $h{errstr}, " at character ", $h{evp}+1, "\n";
    return undef;
  }
  1;
}

sub mimeobj_from_db {
  my ($mail_id,$ctxt)=@_;

  my $sth=$ctxt->{dbh}->prepare("SELECT bodytext FROM body WHERE mail_id=?");
  $sth->execute($mail_id);
  my ($body)=$sth->fetchrow_array;

  $sth=$ctxt->{dbh}->prepare("SELECT lines FROM header WHERE mail_id=?");
  $sth->execute($mail_id);
  my ($headers)=$sth->fetchrow_array;

  my $top = MIME::Entity->build(Encoding => '-SUGGEST',
				Charset => 'iso-8859-1',
				Data => $body);

  for my $hl (split (/\n/, $headers)) {
    $top->head->replace($1, $hl);
  }
  return $top;
}

sub log_filter_hit {
  my ($dbh, $ctxt, $expr_id)=@_;
  my $s1=$dbh->prepare("UPDATE filter_expr SET last_hit=now() WHERE expr_id=?");
  $s1->execute($expr_id);

  if (getconf_bool("log_filter_hits", $ctxt->{mailbox_address})) {
    if (!exists $ctxt->{filters_hit_count}) {
      my $sth=$dbh->prepare("INSERT INTO filter_log(expr_id, mail_id, hit_date) VALUES(?,?,now())");
      $sth->execute($expr_id, $ctxt->{mail_id});
      $ctxt->{filters_hit_count}=1;
    }
    else {
      # When storing several filter hits for the same context (which
      # means for the same message), each hit_date is one microsecond after
      # the previous one (now() being constant during the transaction).
      # This is a kludge that will allow us later to order by hit_date and get the
      # hits in the order in which they happened instead of having an additional
      # int column that would waste space.
      my $sth=$dbh->prepare("INSERT INTO filter_log(expr_id, mail_id, hit_date) VALUES(?,?,now()+?*cast('0.000001s' as interval))");
      $sth->execute($expr_id, $ctxt->{mail_id}, $ctxt->{filters_hit_count});
      $ctxt->{filters_hit_count}++;
    }
  }
}

1;
