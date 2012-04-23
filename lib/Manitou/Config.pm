# Copyright (C) 2004-2011 Daniel Verite

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

use strict;

package Manitou::Config;

require Exporter;
use vars qw(@ISA @EXPORT_OK);
use Carp;

@ISA = qw(Exporter);
@EXPORT_OK = qw(getconf getconf_bool add_mbox mailboxes readconf set_common_conf);

my %mbox_conf;

my %default_conf =
  (
   'auto_db_reconnect' => 'yes',
   'alive_interval' => 60, # seconds
   'body_format_flowed' => 'yes',
   'detach_text_plain' => "yes",
   'flush_word_index_interval' => 60*5,
   'flush_word_index_max_queued' => 100,
   'incoming_check_interval' => 60,
   'outgoing_check_interval' => 5,
   'index_words' => "yes",
   'index_words_accent_mode' => "dual", # strip, keep
   'local_delivery_agent' => "sendmail -f \$FROM\$ -t",
   'log_filter_hits' => 'yes',
   'preferred_charset' => "iso-8859-1 iso-8859-15 utf-8",
   'preferred_datetime' => "mtime",
   'security_checks' => "yes",
   'store_filenames' => "yes",
   'no_send' => "no",
   'store_raw_mail' => "no"
  );

# 'strings' as argument type implies a multiline declaration:
# line1 backslash newline line2....
# the arguments are put into an array containing (line1,line2,...)
my %conf_opts =
  (
   'alive_interval' => 'integer',
   'auto_db_reconnect' => 'bool',
   'body_format_flowed' => 'bool',
   'db_connect_string' => 'string',
   'detach_text_plain' => 'bool',
   'exclude_contents' => 'string',
   'incoming_check_interval' => 'integer',
   'incoming_mimeprocess_plugins' => 'strings',
   'incoming_postprocess_plugins' => 'strings',
   'incoming_preprocess_plugins' => 'strings',
   'flush_word_index_interval' => 'integer',
   'flush_word_index_max_queued' => 'integer',
   'index_words' => 'bool',
   'index_words_accent_mode' => 'string',
   'init_sql' => 'strings',
   'local_delivery_agent' => 'program',
   'log_filter_hits' => 'bool',
   'mailfiles_directory' => 'directory',
   'maintenance_plugins' => 'strings',
   'no_send' => 'bool',
   'outgoing_bcc' => 'email',
   'outgoing_check_interval' => 'integer',
   'outgoing_plugins' => 'strings',
   'plugins_directory' => 'directory',
   'postprocess_mailfile_cmd' => 'program',
   'preferred_charset' => 'string',
   'preferred_datetime' => 'string',
   'security_checks' => 'bool',
   'store_filenames' => 'bool',
   'store_raw_mail' => 'bool',
   'tags_incoming' => 'strings',
   'tmpdir' => 'directory'
  );

sub mbox_exists {
  my ($mbox)=@_;
  for my $m (keys %mbox_conf) {
    return 1 if ($mbox eq $m);
  }
  return 0;
}

# $strict: if 1, the option _has_ to be in the mailbox section,
# no overriding by common section or built-in default values
sub getconf {
  my ($option,$mbox,$strict)=@_;
  if (defined($mbox) && defined($mbox_conf{$mbox}->{$option})) {
    return $mbox_conf{$mbox}->{$option};
  }
  elsif (!$strict && defined($mbox_conf{'common'}->{$option})) {
    return $mbox_conf{'common'}->{$option};
  }
  elsif (!$strict && defined($default_conf{$option})) {
    return $default_conf{$option};
  }
  undef;
}

sub getconf_bool {
  my $r=getconf(@_);
  if ($r eq "yes") {
    return 1;
  }
  elsif ($r eq "no") {
    return 0;
  }
  return $r;
}

sub set_common_conf {
  my ($confkey, $val)=@_;
  $mbox_conf{'common'}->{$confkey}=$val;
}

sub check_option_type {
  my ($type,$value)=@_;
  return 0 if ($type eq 'bool' && lc($value) ne 'yes' && lc($value) ne 'no');
  1;				# later
}

sub readconf {
  my ($filename, $err)=@_;
  my $line=0;
  my $cur_mbox;
  my $multiline=0;
  my ($param,$value,$type);
  open(F, "$filename") or die "$filename: $!";
  while (<F>) {
    chomp;
    ++$line;
    $_ =~ s/^\s+//;		# trim leading blanks
    $_ =~ s/\s+$//;		# trim trailing blanks
    next if (/^\#/ || /^$/);	# comment or empty line
    if ($multiline) {
      if (/(.*)\s*\\$/) {
	$value=$1;
      }
      else {
	$value=$_;
	$multiline=0;
      }
      push @{$mbox_conf{$cur_mbox}->{$param}}, $value;
    }
    elsif (/^\[(.*)\]/) {		# mailbox name
      if (mbox_exists(lc($1))) {
	$err->{msg} = "redundant definition of mailbox '$1' started at line $line";
	return 0;
      }
      else {
	$cur_mbox=lc($1);
      }
    }
    elsif (/^([a-zA-z0-9_]+)\s*=\s*(.*)$/) { # parameter name
      if (!defined($cur_mbox)) {
	$err->{msg} = "a mailbox name enclosed in square brackets was expected above parameter at line $line";
	return 0;
      }
      ($param,$value)=($1,$2);
      $type = $conf_opts{$param};
      if (!defined($type)) {
	$err->{msg} = "unknown parameter '$param' at line $line";
	return 0;
      }
      if ($value =~ /(.*)\s*\\$/) {
	$value=$1;
	if ($type ne "strings") {
	  $err->{msg} = "parameter '$param' does not accept a multilines value at line $line";
	  return 0;
	}
	$multiline=1;
      }
      if (!check_option_type($type, $value)) {
	$err->{msg} = "illegal value for option '$param' at line $line. The variable is expected to be of type '$type'";
	return 0;
      }
      if ($type ne 'strings') {
	$mbox_conf{$cur_mbox}->{$param}=$value;
      }
      else {
	push @{$mbox_conf{$cur_mbox}->{$param}}, $value;
      }
    }
    else {
      $err->{msg} = "Unrecognized contents at line $line";
      return 0;
    }
  }
  close(F);
  return 1;
}

sub add_mbox {
  my ($name, $id)=@_;
  if (!defined $name) {
    die "Argument 'name' must be defined";
  }
  $mbox_conf{$name}->{identity_id}=$id; # $id may be undef
}

sub get_identity_id {
  my ($dbh,$email)=@_;
  if (exists $mbox_conf{$email}) {
    return $mbox_conf{$email}->{identity_id};
  }
  else {
    my $s = $dbh->prepare("SELECT identity_id FROM identities WHERE email_addr=?");
    $s->execute($email);
    my @r = $s->fetchrow_array();
    if (@r) {
      $mbox_conf{$email}->{identity_id}=$r[0];
      return $r[0];
    }
  }
  undef;
}

sub mailboxes {
  return keys %mbox_conf;
}

1;
