# no_duplicate plugin for Manitou-Mail
# Incoming preprocess plugin to discard messages
# already in the database based on their SHA1 digest

# Copyright (C) 2014-2016 Daniel Verite
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
# configuration file, and again as a postprocess plugin to benefit
# from the (fingerprint,mail_id) association
# Init with {'update_tags'=>1} to replace tags of the old message
# by the tags of the new identical incoming message that is skipped.
# Init with 'ignore_identities' set to 1 to deduplicate on fingerprints
# independently of mail.identity_id
# If 'ignore_identities' is set to 0, which is the default case, duplicates
# are considered per-identity, but the plugin *must* be declared
# as a postprocess plugin for the deduplication to work at all.

package Manitou::Plugins::no_duplicate;

use strict;

use Digest::SHA;
use DBD::Pg qw(:pg_types);

sub init {
  my $dbh=shift;
  my ($args)=@_;
  my $self={};
  bless $self;
  foreach ("update_tags", "ignore_identities") {
    $self->{$_} = $args->{$_} if (defined $args->{$_});
  }

  my $sth=$dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name='no_duplicate_import'");
  $sth->execute();
  my @r=$sth->fetchrow_array;
  if (!@r) {
    $dbh->begin_work;

    # Can't have sha1_digest as a primary key, because we might accept duplicates
    # across identities.
    # The unique constraint would be (sha1,identity_id) if the identity was
    # in that table, but it resides in mail.
    $dbh->do("CREATE TABLE no_duplicate_import(sha1_digest bytea, mail_id int)");

    # Non-unique index
    $dbh->do("CREATE INDEX no_duplicate_import_idx ON no_duplicate_import(sha1_digest)");

    # trigger to remove entries on deletion
    $dbh->do(q{CREATE FUNCTION no_duplicate_import_del() RETURNS trigger AS $$ BEGIN DELETE FROM no_duplicate_import WHERE mail_id=OLD.mail_id; RETURN NEW; END $$ language plpgsql});
    $dbh->do(q{CREATE TRIGGER no_duplicate_import_trigger AFTER DELETE ON mail FOR EACH ROW EXECUTE PROCEDURE no_duplicate_import_del()});
    $dbh->commit;
  }
  return $self;
}

sub finish {
  # nothing to do
  1;
}

sub reassign_tags {
  my ($dbh, $mail_id, $tagnames) = @_;

  my @values;
  foreach (@{$tagnames}) {
    my $tag_id = Manitou::Tags::tag_id_from_name($dbh, $_);
    push @values, "($tag_id)" if ($tag_id);
  }

  if (@values) {
    my $sth = $dbh->prepare("SELECT m.tag,l.i FROM (select tag FROM mail_tags WHERE mail_id=?) AS m FULL JOIN (values ".join(',', @values) . ") AS l(i) ON m.tag=l.i WHERE m.tag IS DISTINCT FROM l.i");
    $sth->execute($mail_id);
    my @ins;
    my @del;
    while (my @r = $sth->fetchrow_array) {
      # when m.tag is not null, remove that tag
      # when m.tag is null, insert the tag l.i
      push @del, $r[0] if ($r[0]);
      push @ins, $r[1] if (!$r[0] && $r[1]); # normally, $r[0] null implies $r[1] not null
    }
    $dbh->begin_work if (@del && @ins);
    if (@del) {
      my $s = $dbh->prepare("DELETE FROM mail_tags WHERE mail_id=? AND tag IN (" .
			    join(',', @del) . ")");
      $s->execute($mail_id);
      $s->finish;
    }
    if (@ins) {
      my $s = $dbh->prepare("INSERT INTO mail_tags(mail_id,tag) SELECT ?,i FROM".
			    " unnest('{". join(',', @ins) . "}'::int[]) AS l(i)");
      $s->execute($mail_id);
      $s->finish;
    }
    $dbh->commit if (@del && @ins);
  }
}

sub process {
  my ($self,$ctxt)=@_;

  if ($ctxt->{stage} eq "preprocess" && defined $ctxt->{filename}) {
    # When called in the preprocess stage, check if another mail with the same
    # fingerprint already exists.
    # If yes, set the status to -1 so that the new mail gets discarded.
    open(my $fh, $ctxt->{filename}) or die "cannot open $ctxt->{filename}: $!";
    binmode $fh;
    my $sha1 = Digest::SHA->new("SHA-1");
    $sha1->addfile($fh);
    $ctxt->{sha1_digest} = $sha1->digest;
    my $sth;
    if ($self->{ignore_identities}) {
      $sth = $ctxt->{dbh}->prepare(q{
	INSERT INTO no_duplicate_import(sha1_digest) SELECT $1 WHERE NOT EXISTS
	  (SELECT 1 FROM no_duplicate_import WHERE sha1_digest=$1)
      });
    }
    else {
      $sth = $ctxt->{dbh}->prepare(q{
	INSERT INTO no_duplicate_import(sha1_digest)
	  SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM no_duplicate_import JOIN mail USING(mail_id)
	    WHERE sha1_digest=$1 AND identity_id=$2)
      });
    }
    $sth->bind_param('$1', $ctxt->{sha1_digest}, { pg_type=>DBD::Pg::PG_BYTEA });
    $sth->bind_param('$2', $ctxt->{identity_id}) if (!$self->{ignore_identities});
    $sth->execute;
    # tell the caller to discard the message if the digest already exists.
    if (!$sth->rows) {
      $ctxt->{status}=-1;

      # Optionally, reassign the new tags to the already existing identical copy
      # of the message.
      if ($self->{update_tags}) {
	my $sth = $ctxt->{dbh}->prepare('SELECT mail_id FROM no_duplicate_import WHERE sha1_digest=$1');
	$sth->bind_param('$1', $ctxt->{sha1_digest}, { pg_type=>DBD::Pg::PG_BYTEA });
	$sth->execute;
	my ($mail_id) = $sth->fetchrow_array;
	$sth->finish;
	reassign_tags($ctxt->{dbh}, $mail_id, $ctxt->{tags}) if ($mail_id);
      }
    }
  }
  elsif ($ctxt->{stage} eq "postprocess" && $ctxt->{mail_id}) {
    # When called in the postprocess stage, associate the new mail_id to the fingerprint.
    # This could not be done in the previous stage because the mail_id was not generated yet.
    my $sth = $ctxt->{dbh}->prepare(q{
	UPDATE no_duplicate_import SET mail_id=$1 WHERE sha1_digest=$2 AND mail_id IS NULL
    });
    $sth->bind_param('$1', $ctxt->{mail_id});
    $sth->bind_param('$2', $ctxt->{sha1_digest}, { pg_type=>DBD::Pg::PG_BYTEA });
    $sth->execute;
    if ($sth->rows==0) {
      my $sth1 = $ctxt->{dbh}->prepare('INSERT INTO no_duplicate_import(mail_id,sha1_digest) VALUES($1,$2)');
      $sth1->bind_param('$1', $ctxt->{mail_id});
      $sth1->bind_param('$2', $ctxt->{sha1_digest}, { pg_type=>DBD::Pg::PG_BYTEA });
      $sth1->execute;
      $sth1->finish;
    }
    $sth->finish;
  }
  1;
}

1;
