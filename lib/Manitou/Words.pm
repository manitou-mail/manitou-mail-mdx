# Copyright (C) 2004-2010 Daniel Verite

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

package Manitou::Words;

use strict;
use vars qw(@ISA @EXPORT_OK);
use Carp;

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(load_stopwords index_words flush_word_vectors
		clear_word_vectors queue_size last_flush_time search);

use DBD::Pg qw(:pg_types);
use Bit::Vector;
use integer;

# cache for indexed words
my %hwords;

# Cache for bit vectors. The hash is two-level deep
# $vecs{$widx}->{$part_no} => vector for the word of index $widx and
# for the messages whose mail_id fit in the $part_no partition
my %vecs;

my %no_index_words;

# The size of partitions.
my $partsize;

# The queue of mails whose word vectors haven't been flushed the db yet
my @flush_queue;

# Time of last flush
my $last_flush_time=time;

sub queue_size {
  return scalar(@flush_queue);
}

sub load_partsize {
  my $dbh=shift;
  my $s=$dbh->prepare("SELECT rt_value FROM runtime_info WHERE rt_key='word_index_partsize'") or croak $dbh->errstr;
  $s->execute or croak $dbh->errstr;
  ($partsize) = $s->fetchrow_array;
  # If no 'word_index_partsize' entry, we assume a default value, but 
  # only if nothing has been indexed yet
  if ($s->rows==0) {
    $s=$dbh->prepare("SELECT word_id FROM inverted_word_index LIMIT 1") or croak $dbh->errstr;
    $s->execute or croak $dbh->errstr;
    if ($s->rows!=0) {
      croak "Fatal error: unable to find a 'word_index_partsize' entry in the RUNTIME_INFO table and INVERTED_WORD_INDEX is not empty\n";
    }
    $partsize=16384;		# default value
    $dbh->do("INSERT INTO runtime_info(rt_key,rt_value) VALUES('word_index_partsize','16384')") or croak $dbh->errstr;
  }
}

sub load_stopwords {
  my $dbh=shift;
  my $sth=$dbh->prepare("SELECT wordtext FROM non_indexable_words") or croak $dbh->errstr;
  $sth->execute or croak $dbh->errstr;
  while (my @row=$sth->fetchrow_array) {
    $no_index_words{$row[0]}=1;
  }
  $sth->finish;
}

sub flush_word_vectors {
  my $dbh=shift;

  my $sthu=$dbh->prepare("UPDATE inverted_word_index SET mailvec=?,nz_offset=? WHERE word_id=? AND part_no=?") or croak $dbh->errstr;

  my $sthi=$dbh->prepare("INSERT INTO inverted_word_index(word_id,part_no,mailvec,nz_offset) VALUES (?,?,?,?)") or croak $dbh->errstr;

  my $bits_written=0;

  foreach my $wid (keys %vecs) {
    foreach my $part (keys %{$vecs{$wid}}) {
      next if (!exists $vecs{$wid}->{$part}->{dirty});
      my $bits;
      my $v = $vecs{$wid}->{$part}->{v};
      my ($min,$max) = $v->Interval_Scan_inc(0);
      my $nz_offset=0;
      if ($min>0) {
	$nz_offset=$min/8;
	my $v2 = Bit::Vector->new(0);
	$v2->Interval_Substitute($v, 0, 0,
				 $nz_offset*8, ($v->Size()-$nz_offset*8));
	$bits=$v2->Block_Read();
      }
      else {
	$bits=$v->Block_Read();
      }
      if (defined $vecs{$wid}->{$part}->{insert}) {
	#insert
	$sthi->bind_param(1, $wid);
	$sthi->bind_param(2, $part);
	$sthi->bind_param(3, $bits, { pg_type => DBD::Pg::PG_BYTEA });
	$sthi->bind_param(4, $nz_offset);
	$sthi->execute or croak $dbh->errstr;
	delete $vecs{$wid}->{$part}->{insert};
      }
      else {
	# update
	$sthu->bind_param(1, $bits, { pg_type => DBD::Pg::PG_BYTEA });
	$sthu->bind_param(2, $nz_offset);
	$sthu->bind_param(3, $wid);
	$sthu->bind_param(4, $part);
	$sthu->execute or croak $dbh->errstr;
      }
      delete $vecs{$wid}->{$part}->{dirty};
      $bits_written += length($bits);
    }
  }
  my $sthd=$dbh->prepare("DELETE FROM jobs_queue WHERE mail_id=? AND job_type='widx'");
  foreach (@flush_queue) {
    $sthd->execute($_);
  }
  @flush_queue=();
  $last_flush_time=time;
}

sub last_flush_time {
  return $last_flush_time;
}

sub clear_word_vectors {
#    foreach my $wid (keys %vecs) {
#      foreach my $part (keys %{$vecs{$wid}}) {
#        $vecs{$wid}->{$part}->{v}->Resize(0);
#      }
#    }
  %vecs=();
  %hwords=();
}

# Clear the bits corresponding to a mail_id in the inverted word
# index cache in memory.
# Normally, this is always the last inserted mail, after is has been
# rolled back
# It's important to delete the vector if it's empty because the word
# entry may not exist at all in the database and there's an FK constraint
# from table inverted_word_index referencing table words.
sub clear_last_indexed_mail {
  my ($dbh,$mail_id)=@_;
  load_partsize($dbh) if !defined($partsize);
  my $part_no = $mail_id / $partsize;
  my $bit_id = ($mail_id-1) % $partsize;

  foreach my $wid (keys %vecs) {
    if (exists $vecs{$wid}->{$part_no}->{dirty}) {
      my $vec=$vecs{$wid}->{$part_no}->{v};
      if ($vec->Size()>$bit_id && $vec->bit_test($bit_id)) {
	$vec->Bit_Off($bit_id);
	if ($vec->is_empty()) {
	  delete $vecs{$wid}->{$part_no};
	}
	else {
	  $vecs{$wid}->{$part_no}->{v}=$vec;
	}
      }
    }
  }
  # since INSERT INTO words may have been rolled back,
  # the cache of word_id can no longer be trusted, so it's reset
  %hwords=();
}

sub index_words {
  my ($dbh, $mail_id, $pbody, $pheader)=@_;
  load_partsize($dbh) if !defined($partsize);

  my $cnt=0;
  my $sth_seq = $dbh->prepare("SELECT nextval('seq_word_id')");
  my $sth_w = $dbh->prepare("SELECT word_id FROM words WHERE wordtext=?");
  my $sth_n = $dbh->prepare("INSERT INTO words(word_id,wordtext) VALUES (?,?)");
  my $svec = $dbh->prepare("SELECT mailvec,nz_offset FROM inverted_word_index WHERE word_id=? AND part_no=?");

  my @words=split(/\W/, $$pbody);
  push @words, split(/\W/, $$pheader) if (defined $pheader);

  # hbody_words contains a unique entry for each word occurring in the
  # body, and is used to avoid inserting multiple (word_id,mail_id) tuples.
  my %hbody_words;

  for my $s (@words) {
    my $word_id;
    next if (length($s)<=2 || length($s)>50);
#    next if ($s =~ /^[0-9]+$/);
    $s = lc($s);
    next if (defined($no_index_words{$s}) or defined($hbody_words{$s}));
    $hbody_words{$s}=1;

    # Find the word_id
    if (!defined($hwords{$s})) {
      # The word hasn't been encountered before in any mail
      $sth_w->execute ($s);
      my @r=$sth_w->fetchrow_array;
      if (@r) {
	$word_id=$r[0];
	$hwords{$s}=$word_id;
      }
      else {
	# The word isn't in the words table: let's insert it
	$sth_seq->execute;
	my @ts = $sth_seq->fetchrow_array;
	$sth_n->execute($ts[0],$s);
	$word_id=$ts[0];
	$hwords{$s}=$word_id;
      }
    }
    else {
      $word_id=$hwords{$s};
    }

    # Find the vector
    my $part_no = $mail_id / $partsize;
    my $bit_id = ($mail_id-1) % $partsize;
    my $vec = $vecs{$word_id}->{$part_no}->{v};
    if (!defined $vec) {
      $svec->execute($word_id, $part_no) or croak $dbh->errstr;
      if ($svec->rows>0) {
	# found in db
	my @r=$svec->fetchrow_array;
	my $bits = "\000"x$r[1] . $r[0];
	$vec = Bit::Vector->new(length($bits)*8);
	$vecs{$word_id}->{$part_no}->{v} = $vec;
	$vec->Block_Store($bits);
      }
      else {
	$vec = Bit::Vector->new($bit_id+1);
	$vecs{$word_id}->{$part_no}->{v} = $vec;
	$vecs{$word_id}->{$part_no}->{insert} = 1; # not in db yet
	$vecs{$word_id}->{$part_no}->{dirty} = 1;
      }
    }
    $vec->Resize($bit_id+1) if ($vec->Size()<$bit_id+1);
    if (!$vec->bit_test($bit_id)) {
      $vec->Bit_On($bit_id);
      $vecs{$word_id}->{$part_no}->{dirty} = 1;
    }
    $cnt++;
  }

  if ($cnt) {
    push @flush_queue, $mail_id;
  }
#  my $sthi=$dbh->prepare("INSERT INTO word_indexed_mail(mail_id) VALUES (?)") or croak $dbh->errstr;
#  $sthi->execute($mail_id) or croak $dbh->errstr;
#  $sthi->finish;
}

sub fetch_vec {
  my ($sth_w, $svec, $vr, $w)=@_;

  $sth_w->execute($w) or croak $sth_w->errstr;
  my ($wid)=$sth_w->fetchrow_array;
  return 0 if (!$wid);
  $svec->execute($wid) or croak $svec->errstr;
  while (my ($vecbits,$part,$nz_offset) = $svec->fetchrow_array) {
    my $bits = "\000"x$nz_offset . $vecbits;
    my $v = Bit::Vector->new(length($bits)*8);
    $v->Block_Store($bits);
    if (defined $vr->{$part}) {
      if ($vr->{$part}->Size() < $v->Size()) {
	$v->Resize($vr->{$part}->Size());
      } elsif ($vr->{$part}->Size() > $v->Size()) {
	$vr->{$part}->Resize($v->Size());
      }
      $vr->{$part}->And($vr->{$part}, $v);
    } else {
      $vr->{$part}=$v;
    }
  }
  return 1;
}

sub vecs_to_mailid {
  my $vr=shift;
  my @result;
  foreach my $p (sort (keys %{$vr})) {
    foreach my $bit ($vr->{$p}->Index_List_Read()) {
       # each bit number is a (mail_id/partsize)-1
      push @result, $bit+($p*$partsize)+1;
    }
  }
  return @result;
}

sub and_lists {
  my ($list_one,$list_two) = @_;
  my %temp = ();
  @temp{@$list_one} = (1) x @$list_one;
  return grep $temp{$_}, @$list_two;
}

sub search {
  my $dbh=shift;
  load_partsize($dbh) if !defined($partsize);
  my ($nb_substrings, $nb_words);
  my $sth_w = $dbh->prepare("SELECT word_id FROM words WHERE wordtext=?") or croak $dbh->errstr;
  my $svec = $dbh->prepare("SELECT mailvec,part_no,nz_offset FROM inverted_word_index WHERE word_id=?") or croak $dbh->errstr;

  my %vres;
  my @res_substrings;
  foreach my $w (@_) {
    my @words=split /\W/, $w;
    if (@words==1) {
      # search for one individual word
      $nb_words++;
      fetch_vec($sth_w, $svec, \%vres, $w);
    }
    else {
      # search for a substring
      # first, reduce the search space to the words within the string
      # then intersect with the mails containing the exact string in
      # the mail bodies
      my %vr;
      foreach (@words) {
	fetch_vec($sth_w, $svec, \%vr, $_) if (length($_)>2);
      }
      my $mid=join(',', vecs_to_mailid(\%vr));
      if ($mid) {
	my $qw=$dbh->quote("%$w%");
	my $s=$dbh->prepare("SELECT mail_id FROM body WHERE bodytext ilike $qw AND mail_id IN ($mid) ORDER BY mail_id") or croak $dbh->errstr;
	$s->execute or croak $dbh->errstr;
	my @r;
	while (my @rr=$s->fetchrow_array) {
	  push @r, $rr[0];
	}
	if ($nb_substrings>0) {
	  @res_substrings = and_lists(\@res_substrings, \@r);
	}
	else {
	  @res_substrings=@r;
	}
	$nb_substrings++;
      }
    }
  }

  if ($nb_substrings>0) {
    if ($nb_words>0) {
      my @results=vecs_to_mailid(\%vres);
      return and_lists(\@res_substrings, \@results);
    }
    else {
      return @res_substrings;
    }
  }
  else {
    return vecs_to_mailid(\%vres);
  }
}

# Given a fully decoded mail header, extract the strings that
# should be full-text indexed along with the contents of
# the mail
# Currently, we're extracting only the subject line
sub header_contents_to_ftidx {
  if ($_[0] =~ /^Subject: (.*)$/im) {
    return $1;
  }
  undef;
}


sub flush_jobs_queue {
  my $dbh=shift;
  my $sth=$dbh->prepare("SELECT job_id,mail_id FROM jobs_queue WHERE job_type='widx'") or die $dbh->errstr;
  my $sthb=$dbh->prepare("SELECT bodytext FROM body WHERE mail_id=?");
  my $sthh=$dbh->prepare("SELECT lines FROM header WHERE mail_id=?");
  $sth->execute or die $dbh->errstr;
  while (my ($job_id,$mail_id)=$sth->fetchrow_array) {
    $sthb->execute($mail_id) or die $dbh->errstr;
    my ($body)=$sthb->fetchrow_array;
    $sthh->execute($mail_id) or die $dbh->errstr;
    my ($headers)=$sthh->fetchrow_array;
    index_words($dbh, $mail_id, \$body, \$headers);
  }
  if (queue_size()>0) {
    flush_word_vectors($dbh);
    clear_word_vectors;
  }
}
1;
