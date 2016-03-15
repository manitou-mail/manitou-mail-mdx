# Copyright (C) 2004-2016 Daniel Verite

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


sub encode_qp_char {
  my ($c, $charset) = @_;
  my $res;
  if (($c =~ /[_\?\=\x{0}-\x{1F}]/) || ord($c) >= 127) {
    my $bytes = Encode::encode($charset, $c);
    foreach my $b (split //, $bytes) {
      $res .= sprintf("=%02X", ord($b))
    }
  }
  else {
    $res .= (ord($c)==32 ? '_' : $c);
  }
  $res;
}

sub encode_qp_word {
  my ($word, $charset) = @_;
  my $res;
  foreach my $c (split //, $word) {
    $res .= encode_qp_char($c, $charset);
  }
  $res;
}

# Encode a long expression into multiple mime-encoded words,
# enforcing $limit as the max length of the first ME-word
# and 75 for the next ME-words
sub encode_qp_multiple_words {
  my ($word, $charset, $limit) = @_;
  my @list;
  my $enc;
  my $dec;
  foreach my $c (split //, $word) {
    my $q = encode_qp_char($c, $charset);
    if (length($enc)+length($q) > $limit) {
      push @list, { v=>$enc, d=>$dec, t=>'M' };
      $enc = $q;
      $dec = $c;
    }
    else {
      $enc .= $q;
      $dec .= $c;
    }
  }
  push @list, { v=>$enc, d=>$dec, t=>'M' } if (defined $enc);
  return @list;
}

sub assemble_rfc2047 {
  my ($l, $reserved, $charset, $encoding)=@_;
  my $maxl = 78;
  my $mw_len = length("=?$charset?$encoding?")+2;

  # combine ME-words separated by spaces
  for (my $i=0; $i < @{$l}; $i++) { # @{$l} is modified inside the loop
    my $s = $l->[$i];
    if ($s->{t} eq 'S') {
      if ($i>0 && $l->[$i-1]->{t} eq 'M' && $i+1<@{$l} && $l->[$i+1]->{t} eq 'M') {
	my $txt = $l->[$i-1]->{d} . $s->{v} . $l->[$i+1]->{d};
	my %ins = ( t=>'M', d=>$txt, v=>encode_qp_word($txt, $charset) );
	splice @{$l}, $i-1, 3, (\%ins);
	$i -= 2;   # stay on the current element
      }
    }
  }

  # split long ME words
  for (my $i=0; $i < @{$l}; $i++) { # @{$l} is modified inside the loop
    my $s = $l->[$i];
    if ($s->{t} eq 'M') {
      if (length($s->{v}) + $mw_len > 75) {
	 # ME-word is too big, split it
	my @ins = encode_qp_multiple_words($s->{d}, $charset, 75-$mw_len);
	splice @{$l}, $i, 1, @ins;
      }
    }
  }

  # Fold
  my $llen = 0;
  my $mxl = $maxl - $reserved;
  for (my $i=0; $i < @{$l}; $i++) {   # @{$l} is modified inside the loop
    my $s = $l->[$i];
    my $v = $s->{v};

    if ($s->{t} eq 'M') {
      $v = "=?$charset?$encoding?" . $v . "?=";
    }

    if ($s->{t} eq 'M' || $s->{t} eq 'V') {
      if ($llen + length($v) > $mxl) {
	if ($i>0 && $l->[$i-1]->{t} eq 'S') {
	  # convert spaces to folding spaces
	  $l->[$i-1]->{t} = 'N';
	  $llen = length($l->[$i-1]->{v}) + length($v);
	}
	elsif ($i>0 && $l->[$i-1]->{t} eq 'M') {
	  splice @{$l}, $i, 0, { t=>'N', v=>' ', d=>' ' };
	  $llen = 1;
	  $mxl = $maxl;
	}
	else {
	  $llen = length($v)+ 2;
	  $mxl = $maxl;
	}
      }
      else {
	$llen += length($v);
      }
    }
    else {
      $llen += length($v);
    }
  }
  return @{$l};
}

sub encode_rfc2047_header {
    my ($value, $in_charset, $reserved) = @_;
    # $reserved is the number of bytes already used at the beginning of the
    # header. Used to check against the rfc822 78/998 line length limits.

    my $max_bytes = 78-$reserved;
    my $charset  = uc($in_charset);
    my $encoding = "Q";

    # shortcut if short line containing only printable US-ASCII
    if ($value !~ /[^\x20-\x7E]/ && length($value) < $max_bytes) {
	return $value;
    }

    # fold the big line into several lines separated by newline+space
    my @tokens = split (/( +)/, $value);

    # if the line starts with WSPs, the first array elt will be undef: remove it
    shift @tokens if (@tokens>0 && $tokens[0] eq "");

    my @out;
    for (my $i=0; $i < @tokens; $i++) {
      my $w = $tokens[$i];
      my $wl = length $w;
      if ($w =~ / /o) {
	# space(s)
	push @out, { t=>'S', v=>$w };
      }
      else {
	# word (non WSP)
	if ($w =~ /[^\x{20}-\x{7e}]/o) {
	  # word has characters outside of US-ASCII
	  my $w0 = $w;
	  $w = encode_qp_word($w, $charset);
	  push @out, { t=>'M', v=>$w, d=>$w0 };
	}
	else {
	  # word that doesn't need MIME encoding
	  push @out, { t=>'V', v=>$w };
	}
      }
    }

    # Reassemble the words
    assemble_rfc2047(\@out, $reserved, $charset, $encoding);

    my $llen = 0;
    my $mline;			# multi-line output
    foreach (@out) {
      my $v = $_->{v};
      if ($_->{t} eq "M") {
	$v = "=?$charset?$encoding?" . $v . "?=";
	$mline .= $v;
      }
      elsif ($_->{t} eq "V") {
	$mline .= $v;
      }
      elsif ($_->{t} eq "N") {
	$mline .= "\n" . $v;
	$llen = 0;
      }
      elsif ($_->{t} eq "S") {
	$mline .= $v;
      }
      $llen += length $v;
    }

    return $mline;
}

sub encode_header {
  my $top=shift;
  my $header_lines=shift;
  my @charsets=@_;

  my $hln=0;
  # do not automatically reformat headers (Mail::Head's automatic
  # reformatting converts our CRLF+LWSP between mime-encoded word to
  # spaces)
  $top->head->modify(0);
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
      my $v = encode_rfc2047_header($h_line, $eh_charset, length($h_entry)+2);
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
  my $body=shift;
  my @charsets = @_;

  my $sep2="\n";
  local $Text::Wrap::separator2 = "\n";
  local $Text::Wrap::columns = 78;
  local $Text::Wrap::huge = 'overflow';
  my @paragraphs = split(/\n/, $body);
  $body="";
  foreach (@paragraphs) {
    if (length($_)>1 && substr($_,0,1) ne ">") { # don't wrap quoted contents
      $body .= wrap('', '', $_) . "\n";
    }
    else {
      $body .= "$_\n";
    }
  }

  # try the different charsets in the order of their declaration and
  # keep the one with which encode() produces no error
  my $enc_body;
  my $body_charset;
  foreach (@charsets) {
    eval {
      $enc_body = Encode::encode($_, $body, Encode::FB_CROAK|Encode::LEAVE_SRC);
    };
    if (!$@) {
      $body_charset=$_;
      last;
    }
  }

  if (!defined $body_charset) {
    die "Unable to encode body of outgoing mail with any of the specifieds charset (See 'preferred_charset' configuration parameter)";
  }

  return ($enc_body, $body_charset);
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
  my $t = POSIX::mktime($secs, $min, $hour, $day,
			$month-1, $year-1900, 0, 0, -1);
  if (defined $t) {
    # mktime does not fail on certain invalid dates.
    # that's why we convert mktime's result back to a date in
    # localtime and compare that to the day/month/year that
    # we passed. If they're different, that was an invalid date.
    my $ymd = strftime("%Y-%m-%d", localtime($t));
    return undef if ($ymd ne sprintf("%04d-%02d-%02d", $year, $month, $day));
  }
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
