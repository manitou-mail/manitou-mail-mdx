# Copyright (C) 2004-2024 Daniel Verite

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

package Manitou::Encoding;

use strict;

use Encode;
use Carp;
use version;
use MIME::Words qw(:all);

require Exporter;
use vars qw(@ISA @EXPORT_OK);

@ISA = qw(Exporter);
@EXPORT_OK = qw(encode_dbtxt decode_dbtxt header_decode);

# Whether utf-8 Perl strings are converted to bytes before being sent
# to the database
my $pass_utf8_bytes;

# Convert from perl internal format to a string of bytes suitable for
# the current db encoding
sub encode_dbtxt {
  return $_[0] if (!$pass_utf8_bytes);
  return undef if (!length($_[0]));
  return Encode::encode("UTF-8", $_[0], Encode::FB_PERLQQ);
}

# Decode from the current db to perl internal format
sub decode_dbtxt {
  return $_[0] if (!$pass_utf8_bytes && !$_[1]);
  return Encode::decode("UTF-8", $_[0]);
}

sub get_db_encoding {
  my $dbh=shift;
  # determine how our version of DBD::Pg deals with utf-8 strings

  if (version->parse($DBD::Pg::VERSION) >= version->parse("3.6.0")) {
    # DBD::Pg fixed the double-encoding utf-8 bug in version 3.6,
    # so the SELECT below wouldn't discriminate properly.
    # For versions >= 3.6.0, assume its utf-8 support does not require
    # to pass strings as bytes.
    $dbh->{pg_enable_utf8} = 1;
    $pass_utf8_bytes = 0;
  }
  else {
    $dbh->{pg_enable_utf8} = 0;
    my $p = "\xc3\xa9";  # U+00C9 as an utf-8 octet sequence
    my $sth = $dbh->prepare("SELECT ?,length(?),octet_length(?)",
			    {pg_server_prepare=>0});
    $sth->execute($p,$p,$p);
    my @r = $sth->fetchrow_array;
    if ($r[1]==1 && $r[2]==2) {
      # keep it that way (pre DBD-3.0 behavior)
      $pass_utf8_bytes = 1;
      return;
    }
    else {
      $dbh->{pg_enable_utf8} = 1;
      $pass_utf8_bytes = 0;
    }
  }
}

sub header_decode {
  my $h;
  foreach (decode_mimewords($_[0])) {
    my @t=@{$_};
    # default to iso-8859-15 if no encoding is specified in the header
    # or if it's invalid. Normally this should be us-ascii but we're
    # more permissive to avoid rejecting malformed messages containing
    # 8 bit characters in headers.
    $t[1]='iso-8859-15' if (!defined ($t[1]) || !Encode::resolve_alias($t[1]));
    eval {
      $h .= Encode::decode($t[1], $t[0]);
    };
    if ($@) {
      # if the decode fails (typically if the charset is unknown)
      # we fall down to using the string as is
      $h.=$t[0];
    }
  }
  return $h;
}

sub header_decode_unfold {
  my $h;
  foreach (decode_mimewords($_[0])) {
    my @t=@{$_};
    # default to iso-8859-15 if no encoding is specified in the header
    # or if it's invalid. Normally this should be us-ascii but we're
    # more permissive to avoid rejecting malformed messages containing
    # 8 bit characters in headers.
    $t[1]='iso-8859-15' if (!defined ($t[1]) || !Encode::resolve_alias($t[1]));
    $t[0] =~ s/\r?\n[\t ]/ /sog;	# unfold
    eval {
      $h .= Encode::decode($t[1], $t[0]);
    };
    if ($@) {
      # if the decode fails (typically if the charset is unknown)
      # we fall down to using the string as is
      $h.=$t[0];
    }
  }
  return $h;
}

1;
