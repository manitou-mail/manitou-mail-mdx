# Copyright (C) 2004-2012 Daniel Verite

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

require Encode;
use Carp;
use MIME::Words qw(:all);

require Exporter;
use vars qw(@ISA @EXPORT_OK);

@ISA = qw(Exporter);
@EXPORT_OK = qw(encode_dbtxt decode_dbtxt header_decode);

# The encoding name, as advertised by the database
my $db_encoding;

sub get_db_encoding {
  my $dbh=shift;
  my $sthe = $dbh->prepare("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname=current_database()") or croak $dbh->errstr;
  $sthe->execute or croak $sthe->errstr;
  ($db_encoding) = $sthe->fetchrow_array;
  if ($db_encoding eq "UNICODE") {
    # pre-8.1 pgsql returns "UNICODE", we prefer the newer "UTF8"
    $db_encoding="UTF8";
  }
  $sthe->finish;
}

# Convert from perl internal format to a chain of bytes suitable for
# the current db encoding
sub encode_dbtxt {
  return undef if (!length($_[0]));
  return Encode::encode("utf8", $_[0], Encode::FB_PERLQQ);
}

# Decode from the current db to perl internal format
sub decode_dbtxt {
  return Encode::decode("utf8", $_[0]);
}

sub header_decode {
  my $h;
  foreach (decode_mimewords($_[0])) {
    my @t=@{$_};
    # default to iso-8859-15 if no or an invalid encoding is specified in the
    # header. Normally this should be us-ascii but we're more permissive
    # to avoid rejecting malformed messages containing 8 bit
    # characters in headers.
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

1;
