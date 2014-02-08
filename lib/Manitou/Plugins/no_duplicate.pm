# no_duplicate plugin for Manitou-Mail
# Incoming preprocess plugin to discard messages
# already in the database based on their SHA1 digest

# Copyright (C) 2014 Daniel Verite
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

### HOW TO USE ###
# Declare the plugin as an incoming preprocess plugin in manitou-mdx
# configuration file.

package Manitou::Plugins::no_duplicate;

use strict;

use Digest::SHA;
use DBD::Pg qw(:pg_types);

sub init {
  my $dbh=shift;
  my ($args)=@_;
  my $self={};
  bless $self;

  my $sth=$dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name='no_duplicate_import'");
  $sth->execute();
  my @r=$sth->fetchrow_array;
  if (!@r) {
    $dbh->do("CREATE TABLE no_duplicate_import(sha1_digest bytea primary key)");
  }
  return $self;
}

sub finish {
  # nothing to do
  1;
}

sub process {
  my ($self,$ctxt)=@_;
  if (defined $ctxt->{filename}) {
    open(my $fh, $ctxt->{filename}) or die "cannot open $ctxt->{filename}: $!";
    binmode $fh;
    my $sha1 = Digest::SHA->new("SHA-1");
    $sha1->addfile($fh);
    $ctxt->{sha1_digest} = $sha1->digest;
    my $sth = $ctxt->{dbh}->prepare('INSERT INTO no_duplicate_import(sha1_digest) SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM no_duplicate_import WHERE sha1_digest=$1)');
    $sth->bind_param('$1', $ctxt->{sha1_digest}, { pg_type=>DBD::Pg::PG_BYTEA });
    $sth->execute;
    # tell the caller to discard the message if the digest already existed
    $ctxt->{status}=-1 if (!$sth->rows);
  }
  1;
}

1;
