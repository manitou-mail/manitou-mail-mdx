# Rewrite the mailfile in place if it happens that every newline of its body
# is duplicated.

package Manitou::Plugins::undouble_newlines;

sub init {
  my $dbh=shift;
  my ($args)=@_;
  my $self={};
  bless $self;
  if ($_[0] eq "delete") {
    $self->{delete}=1;
  }
  return $self;
}

sub finish {
  1;
}

sub process {
  my ($self,$ctxt) = @_;
  my $fname=$ctxt->{filename};
  my $tmpname=$fname."-filtered";
  open(F, $fname) or return undef;
  if (!open(F2, ">$tmpname")) {
    close(F);
    return undef;
  }
  my $in_header=1;
  my $prev_line;
  my $body_lines=0;
  my $ne_line=0;		# first non-empty body line
  my $all_double=1; # if every line of the body is followed by an empty line
  while (<F>) {
    chomp;
    if ($in_header) {
      print F2 "$_\n";
      if ($_ ne "") { # header lines
	next;
      }
      $in_header=0;
      $prev_line="";
    }
    else {
      $body_lines++;
      if ($_ ne "") {
	if ($prev_line ne "") {
	  $all_double=0;	# found two consecutive non empty lines
	  last;
	}
	if ($ne_line==0) { $ne_line=$body_lines; }
	print F2 "$_\n";
      }
      else {
	if (($body_lines-$ne_line) % 2 == 0) {
	  print F2 "\n";
	}
      }
      $prev_line=$_;
    }
  }
  close(F);
  close(F2);
  $all_double=0 if ($body_lines<=1); # not enough data to make a difference
  if (!$all_double) {
    unlink($tmpname);
  }
  else {
    if ($self->{delete}) {
      unlink($fname);
    }
    else {
      rename($fname, "$fname-unfiltered");
    }
    rename($tmpname, $fname);
  }
  1;
}

1;
