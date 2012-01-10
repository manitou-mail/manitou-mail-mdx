# Spamassassin feed for training the bayesian filter
# Plugin for Manitou-Mail
# Copyright (C) 2010 Daniel Verite

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

package Manitou::Plugins::spam_learner;

use DBI;
use Sys::Syslog;
use strict;

# Spam policy implied by this plugin:

# Rule #1: all spam messages detected by the antispam software should
# be moved automatically to the trashcan and assigned a spam tag with
# mail_tags.agent set to NULL (normal behavior for robots)

# Rule #2: all spam messages detected by users are moved to the
# trashcan, with or without being assigned the spam tag

# Rule #3: no legitimate messages are EVER moved to the trashcan, only
# spam (so trashcan=spamcan)

my $plugin="spam_learner";  # for log messages

sub init {
  my $dbh=shift;
  my ($args)=@_;
  my $self={};
  bless $self;

  if (defined $args) {
    if (ref($args) eq "HASH") {
      $self->{spamtag}=$args->{tag} if (defined $args->{tag});
      $self->{max_age} = 10;
      if (defined $args->{max_age}) {
	if ($args->{max_age} !~ /^\d+$/) {
	  die "$plugin: argument max_age must be an integral number (of days)";
	}
	$self->{max_age} = $args->{max_age};
      }
    }
    else { $self->{spamtag}=$args; }
  }

  # Init schema
  # We assume that the caller deals with database errors
  # ($dbh->{RaiseError}==1 and we're called from inside an eval block)
  my $s1=$dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name=? AND table_schema=current_schema()");
  $s1->execute("plugin_spam_learner_history");
  if (scalar($s1->fetchrow_array)==0) {
    $dbh->do(q{CREATE TABLE plugin_spam_learner_history(
      mail_id int,
      status text,
      date_insert timestamptz default now()
      )
    });
    $dbh->do("COMMENT ON column plugin_spam_learner_history.status IS 'FP=false positive, NRA=not recognized automatically, HAM=ham picked up to match a spam to maintain the balance'");
    $dbh->do("CREATE INDEX plugin_spam_learner_history_idx ON plugin_spam_learner_history(mail_id)");
  }
  return $self;
}

sub finish {
  1;
}

sub fetch_list {
  my ($sth,$context) = @_;
  my @list;
  while (my ($fname, $mail_id)=$sth->fetchrow_array) {
    $fname =~ s/\.received$/\.processed/;
    if (-r $fname) {
      push @list, [$mail_id, $fname];
    }
    else {
      $context->{error_log}("$plugin: needs mailfile '$fname' but cannot access it");
    }
  }
  return @list;
}

sub sa_learn {
  my $context=shift;
  my $type=shift;
  my @list;
  foreach (@_) {
    push @list, @{$_}[1];
  }
  if (@list>0) {
    my $cmd = "sa-learn --$type @list";
    my $result = `$cmd`;
    if ($? == 0) {
      $context->{notice_log}($result);
    }
    else {
      $context->{error_log}("error in execution of command: $cmd");
    }
  }
}

sub process {
  my $self = $_[0];
  my $context = $_[1];
#  return 1 if (!$self->{'delay_days'});	# shouldn't happen

  my @ham_list;
  my @spam_list;
  my @fp_list;
  my $in_trans;

  my $dbh = $context->{dbh};

  local $dbh->{RaiseError}=1;
  local $dbh->{PrintError}=1;
  eval {
    my $st1 = $dbh->prepare("select tag_id FROM tags WHERE name=? AND parent_id is null");
    $st1->execute($self->{spamtag});
    my ($tag_id) = $st1->fetchrow_array;
    if (!defined $tag_id) {
      die("$plugin: cannot find tag named '$self->{spamtag}'"); # caught below
    }

    # Extract the SPAM messages that have been recognized by users:
    # - not older than 10 days
    # - not tagged as spam by a robot (agent is null)
    # - in the trashcan (which per policy means that a user has decided it was spam)
    # - not processed previously by this plugin
    my $sth1 = $dbh->prepare("SELECT files.filename, mail.mail_id FROM files JOIN mail USING(mail_id) LEFT JOIN (SELECT mail_id FROM mail_tags WHERE tag=? AND agent IS NULL) mt USING (mail_id) WHERE mt.mail_id IS NULL AND mail.status&16=16 AND msg_date>=now()-?::interval AND mail.mail_id NOT IN (select mail_id FROM plugin_spam_learner_history WHERE status='NRA') ");

    $sth1->execute($tag_id, $self->{max_age}." days");
    @spam_list = fetch_list($sth1, $context);
    $sth1->finish;

    # This query retrieves FALSE POSITIVES:
    # - tagged as spam
    # - NOT in trashcan (=moved out of trashcan)
    # - read
    # - not previously selected
    my $sth3 = $dbh->prepare("SELECT files.filename, mail.mail_id FROM mail JOIN files USING(mail_id) JOIN (SELECT mail_id FROM mail_tags WHERE tag=?) mt USING (mail_id) WHERE mail.status&16=0 AND mail.status&1=1 AND mail.mail_id NOT IN (select mail_id FROM plugin_spam_learner_history WHERE status='FP')");
    $sth3->execute($tag_id);
    @fp_list = fetch_list($sth3, $context);
    $sth3->finish;


    my $max_ham = scalar(@spam_list) - scalar(@fp_list);

    if ($max_ham > 0) {
      # This query picks up N ham messages. They have to be read, not
      # trashed, not tagged as spam, as recent as possible, and not picked up previously
      my $sth2 = $dbh->prepare("SELECT files.filename, mail.mail_id FROM mail JOIN files USING(mail_id) LEFT JOIN (SELECT mail_id FROM mail_tags WHERE tag=?) mt USING (mail_id) WHERE mt.mail_id IS NULL AND mail.status&16=0 AND mail.status&1=1 AND mail.mail_id NOT IN (select mail_id FROM plugin_spam_learner_history WHERE status='HAM') ORDER BY mail.mail_id DESC LIMIT $max_ham");

      $sth2->execute($tag_id);
      @ham_list = fetch_list($sth2, $context);
      $sth2->finish;
    }

    sa_learn($context, "ham", (@fp_list,@ham_list));
    sa_learn($context, "spam", @spam_list);
    `sa-learn --sync`;

    $dbh->begin_work;
    $in_trans=1;
    my $su1=$dbh->prepare("INSERT INTO plugin_spam_learner_history(mail_id,status) VALUES(?,?)");

    # Remove the spam tag from false positives
    my $sthrt = $dbh->prepare("DELETE FROM mail_tags WHERE mail_id=? AND tag=?");

    foreach (@spam_list) {
      $su1->execute(@{$_}[0], 'NRA');
    }
    foreach (@ham_list) {
      $su1->execute(@{$_}[0], 'HAM');
    }
    foreach (@fp_list) {
      $su1->execute(@{$_}[0], 'FP');
      $sthrt->execute(@{$_}[0], $tag_id);
    }
    $dbh->commit;
  };
  if ($@) {
    $dbh->rollback() if ($in_trans);
    $context->{error_log}($@);
    0;
  }
  else {
    $context->{notice_log}("$plugin: learned ". scalar(@spam_list) . " spam(s) " . scalar(@ham_list) . " ham(s) " . scalar(@fp_list) . " false positive(s)");
    1;
  }

}

1;
