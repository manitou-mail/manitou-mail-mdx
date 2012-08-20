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

package Manitou::Database;

use strict;
use vars qw(@ISA @EXPORT_OK);

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(db_connect);

use Manitou::Config qw(getconf);

sub db_connect {
  my $cnx_string=getconf("db_connect_string");
  if (!defined $cnx_string) {
    $cnx_string=$ENV{'MANITOU_CONNECT_STRING'};
  }
  if (!defined $cnx_string) {
    die "Please define the db_connect_string parameter in the configuration file, or the MANITOU_CONNECT_STRING environment variable.";
  }
  my $dbh = DBI->connect($cnx_string) or die "Can't connect: $DBI::errstr";
  $dbh->{PrintError}=1;
  $dbh->{RaiseError}=1;
  $dbh->{pg_auto_escape}=1;
  # We set AutoCommit to 1 to avoid being "idle in transaction" when doing
  # nothing, and we issue begin_work/commit pairs when transactions are needed
  $dbh->{AutoCommit}=1;

  Manitou::Encoding::get_db_encoding($dbh);
  $dbh->do("SET client_encoding=UTF8");
  $dbh->do("LISTEN job_request");

  my @init = getconf("init_sql");
  foreach (@init) {
    $dbh->do($_);
  }
  return $dbh;
}
