# Copyright (C) 2004-2018 Daniel Verite

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
@EXPORT_OK = qw(db_connect bytea_output db_loop_reconnect);

use Manitou::Config qw(getconf);
my $cache_bytea_output;

sub bytea_output {
  $cache_bytea_output;
}

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

  my $s=$dbh->prepare("SELECT setting FROM pg_catalog.pg_settings WHERE name='bytea_output'");
  $s->execute;
  ($cache_bytea_output) = $s->fetchrow_array;
  $cache_bytea_output="escape" if (!defined $cache_bytea_output);

  create_db_temp_functions($dbh);

  return $dbh;
}

# Attempt to reconnect every 5s
# Return undef to give up, but currently it tries forever.
sub db_loop_reconnect {
  my $dbh;
  do {
    sleep 5;
    eval {
      $dbh = db_connect;
    };
  } while ($@);
  return $dbh;
}

sub create_db_temp_functions {
  my $dbh = shift;

  if ($dbh->{pg_server_version} < 90500) {
    # Insert a new email address, handling a potential concurrent session that
    # has inserted the same address and not yet committed.
    # Created only with PG 9.4 and older, otherwise INSERT...ON CONFLICT is used.
    my $r = $dbh->do(q{
      CREATE FUNCTION pg_temp.insert_address(in_email text, in_name text) returns integer as $$
      DECLARE
       v_addr_id int;
      BEGIN
	BEGIN
	  INSERT INTO addresses(addr_id,email_addr,name,last_recv_from,nb_recv_from)
	    VALUES(nextval('seq_addr_id'), in_email, in_name, now(), 1)
	  RETURNING addr_id
	  INTO v_addr_id;

	EXCEPTION WHEN unique_violation THEN
	 SELECT addr_id FROM addresses WHERE email_addr=in_email
	   INTO v_addr_id;
	END;

	RETURN v_addr_id;
      END $$ language plpgsql;
    });
  }
}

1;
