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

package Manitou::MailFormat;

use strict;
use vars qw(@ISA @EXPORT_OK);
use Encode;
use Text::Wrap;
use POSIX qw(strftime locale_h);
use MIME::Words qw(:all);

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(encode_header add_date_header encode_text_body parse_sender_date);

# Fixed version of encode_mimewords, that merges consecutive rfc2047
# encoded words separated by a space.
sub my_encode_mimewords {
    my ($rawstr, $in_charset) = @_;
    my $charset  = uc($in_charset) || 'US-ASCII';
    my $encoding = "Q";
    my $NONPRINT = "\\x00-\\x1F\\x7F-\\xFF";

    ### Encode any "words" with unsafe characters.
    ###    We limit such words to 18 characters, to guarantee that the
    ###    worst-case encoding give us no more than 54 + ~10 < 75 characters
    my $word;
    $rawstr =~ s{([a-zA-Z0-9\x7F-\xFF]{1,18})}{     ### get next "word"
	$word = $1;
	(($word !~ /[$NONPRINT]/o)
	 ? $word                                          ### no unsafe chars
	 : encode_mimeword($word, $encoding, $charset));  ### has unsafe chars
    }xeg;
    $rawstr =~ s/=\?$charset\?$encoding\?(.*)\?=\s+=\?$charset\?$encoding\?(.*)\?=/=?$charset?$encoding?$1_$2?=/g;
    $rawstr;
}

sub encode_header {
  my $top=shift;
  my $header_lines=shift;
  my @charsets=@_;

  my $hln=0;
  for my $hl (split (/\n/, $header_lines)) {
    $hln++;
    chomp $hl;
    # get the pair (header_name,value) into ($1,$2)
    if ($hl =~ /^([^:]+):\s+(.*)/) {
      my $h_entry=$1;
      my $h_line=$2;
      my $eh_line=$h_line;			# encoded header line
      my $eh_charset;
      if ($h_line =~ /[^\x20-\x7F]/) { # if at least one non-ascii character
	foreach (@charsets) {
	  eval {
	    $eh_line = Encode::encode($_, $h_line, Encode::FB_CROAK|Encode::LEAVE_SRC);
	  };
	  if (!$@) {	# encoding is OK
	    $eh_charset=$_;
	    last;
	  }
	}
	if (!defined $eh_charset) {
	  die "Unable to encode outgoing header entry at line $hln with any of the specified charsets (See 'preferred_charset' configuration parameter)";
	}
      }
      my $v = my_encode_mimewords($eh_line, $eh_charset);

      $top->head->replace($h_entry, $v);

    }
    else {
      # we couldn't find 'header_name: header_value'
      warn "Unrecognized header entry '$hl' at line $hln for outgoing mail\n";
    }
  }
}

sub add_date_header {
  my $top=shift;
  # rfc2822 format
  my $old_l = setlocale(LC_TIME, "C");
  my $date = strftime("%a, %d %b %Y %H:%M:%S %z", localtime(time()));
  $top->head->replace("Date", $date);
  setlocale(LC_TIME, $old_l);
}

sub encode_text_body {
  my $db_body=shift;
  my @charsets = @_;

  my $body;
  # try the different charsets in the order of their declaration and
  # keep the one with which encode() produces no error
  my $body_charset;
  foreach (@charsets) {
    eval {
      $body = Encode::encode($_, $db_body, Encode::FB_CROAK|Encode::LEAVE_SRC);
    };
    if (!$@) {
      $body_charset=$_;
      last;
    }
  }

  if (!defined $body_charset) {
    die "Unable to encode body of outgoing mail with any of the specifieds charset (See 'preferred_charset' configuration parameter)";
  }

  my $format_flowed;
  my $sep2="\n";
#  if (getconf_bool("body_format_flowed", $mbox)) {
#    $format_flowed = "; format=flowed";
#    $sep2 = " \n";
#  }
  local $Text::Wrap::separator2=$sep2;
  local $Text::Wrap::columns=78;
  my @paragraphs = split(/\n/, $body);
  $body="";
  foreach (@paragraphs) {
    if (substr($_,0,1) ne ">") { # don't wrap quoted contents
      $body .= wrap('', '', $_) . "\n";
    }
    else {
      $body .= "$_\n";
    }
  }
  return ($body, $body_charset);
}


# Extract fields from a header date
# return undef on parse error or a list:
# ($secs, $min, $hour, $day, $month(1->12), $year(4 digits), $tz_offset(+|-\d{4})
sub parse_sender_date {
  my ($d) = @_;
  my %days = ('sun',1,'mon',2,'tue',3,'wed',4,'thu',5,'fri',6,'sat',7);
  my %months = ('jan',1,'feb',2,'mar',3,'apr',4,'may',5,'jun',6,'jul',7,
                'aug',8,'sep',9,'oct',10,'nov',11,'dec',12);
  my ($year,$month,$day);
  my $strtim;
  $d =~ s/^\s+(.*)/$1/;
  # see if it starts with the day's name
  my @words = split (/\W/, $d, 2);
  my @dat = split (/\s+/, $d);
  if (!$days{lc($words[0])}) {
    # if the day's name is missing, prepend a dummy name
    @dat = ('day,', @dat);
  }
  # if HH:MM:SS or HH:MM comes before the year
  if (defined($dat[3]) && $dat[3] =~ /\d\d\:\d\d/) {
    $year=$dat[4];
    $strtim=$dat[3];
  }
  else {
    $year=$dat[3];
    $strtim=$dat[4]
  }
  if (exists $months{lc($dat[2])}) {
    $month=$months{lc($dat[2])};
    $day=$dat[1];
  }
  elsif (exists $months{lc($dat[1])}) {
    $month=$months{lc($dat[1])};
    $day=$dat[2];
  }
  else {
    return undef;
  }
  return undef if ($day<1 || $day>31);

  my $tz="";
  if ($dat[5] =~ /^[+\-](\d\d)(\d\d)$/) {
    if ($1<=13 && $2<60) { # NZDT is UTC+1300
      $tz=" $dat[5]";
    }
  }
  return undef if ($strtim !~ /^\d{1,2}:\d{1,2}:\d{1,2}$/);
  my ($hour,$min,$secs)= split (/:/, $strtim);
  return undef if ($hour>=24 || $min>=60 || $secs>=60);

  $year += 2000 if ($year<50);
  my $t = POSIX::mktime ($secs, $min, $hour, $day,
                         $month-1, $year-1900);
  return (defined $t) ? ($year,$month,$day,$hour,$min,$secs,$tz) : undef;
}

# Convert a header date to "YYYY-MM-DD HH:MM:SS [+TZ]" format
# Returns undef if it appears impossible
sub reformat_sender_date {
  my ($y,$m,$d,$h,$mn,$s,$tz) = parse_sender_date(@_);
  if (defined $y) {
    return sprintf("%04d-%02d-%02d %02d:%02d:%02d %s",
		   $y, $m, $d, $h, $mn, $s, $tz);
  }
  else {
    return undef;
  }
}

# Convert a header date to an UTC timestamp
# Returns undef on parse error
sub convert_sender_date_to_timestamp {
  my ($y,$m,$d,$h,$mn,$s,$tz) = parse_sender_date(@_);
  if (defined $y) {
    my $t = POSIX::mktime($s, $mn, $h, $d, $m-1, $y-1900);
    return undef if (!defined $t);
    if ($tz =~ /^\s+([+-])(\d{2})(\d{2})$/) {
      if ($1 eq "+") {
	$t -= $2*3600+$3*60;
      }
      elsif ($1 eq "-") {
	$t += $2*3600+$3*60;
      }
    }
    return $t;
  }
  else {
    return undef;
  }
}

1;
