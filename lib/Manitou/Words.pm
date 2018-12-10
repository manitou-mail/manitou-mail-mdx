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

package Manitou::Words;

use strict;
use vars qw(@ISA @EXPORT_OK);
use Carp;

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(load_stopwords index_words flush_word_vectors clear_word_vectors
		queue_size last_flush_time search load_fti_config words_table_name);

use DBD::Pg qw(:pg_types);
use Time::HiRes qw(gettimeofday tv_interval);
use Bit::Vector;
use Manitou::Log qw(error_log notice_log);
use Manitou::Config qw(getconf getconf_bool);
use Manitou::Encoding qw(encode_dbtxt decode_dbtxt);
use Manitou::Attachments;
use Manitou::Database qw(bytea_output);
use Unicode::Normalize;
use HTML::TreeBuilder;
use integer;
use Data::Dumper;

# cache for indexed words
my %hwords;

# Cache for bit vectors. The hash is two-level deep
# $vecs{$widx}->{$part_no} => vector for the word of index $widx and
# for the messages whose mail_id fit in the $part_no partition
my %vecs;
our $vecs_estim_size;		# estimated size in bytes

my %no_index_words;

# Prepared statement handles to lookup words when partitioning is on.
my %cache_sth_w;

# Configuration of the full-text index, to be loaded from the database
# Currently used as a flag 
my $fti_config;

# The size of partitions.
my $partsize;

# Optional partitioning of 'words' table
# Supported values:
# 0: none
# 1: 28 tables based on 1st character, 26 for a..z + 1 for digits + 1 for others
my $words_partitioning_mode;

# When $unaccent is true, we index only the unaccented form of words.
# This allows for accent-insensitive search.
my $unaccent;

# When $add_unaccent is true, accented words are indexed both with and
# without accents. This allows for exact searches of accented words in
# addition to accent-insensitive search.
my $add_unaccent;

# Read the configuration only once
my $accents_configured=0;

# The queue of mails whose word vectors haven't been flushed the db yet
my @flush_queue;

# Time of last flush
my $last_flush_time=time;

my $max_mail_id_index_flush=0;

sub queue_size {
  return scalar(@flush_queue);
}

sub load_fti_config {
  my $dbh=shift;
  my $s=$dbh->prepare(qq{SELECT rt_key, rt_value FROM runtime_info
        WHERE rt_key IN ('word_index_partsize','words_partitioning')});
  $s->execute;
  $words_partitioning_mode = 0;
  $partsize=undef;
  while (my ($k,$v) = $s->fetchrow_array) {
    if ($k eq "word_index_partsize") {
      $partsize=$v;
    }
    elsif ($k eq "words_partitioning" && $v eq "1") {
      $words_partitioning_mode = 1;
    }
  }
  $fti_config = 1;

  # If no 'word_index_partsize' entry, we assume a default value, but 
  # only if nothing has been indexed yet
  if (!$partsize) {
    $s=$dbh->prepare("SELECT word_id FROM inverted_word_index LIMIT 1");
    $s->execute;
    if ($s->rows!=0) {
      croak "Fatal error: unable to find a 'word_index_partsize' entry in the RUNTIME_INFO table and INVERTED_WORD_INDEX is not empty\n";
    }
    $partsize=16384;		# default value
    $dbh->do("INSERT INTO runtime_info(rt_key,rt_value) VALUES('word_index_partsize','16384')");
  }
  return {"partsize"=>$partsize, "words_partitioning"=>$words_partitioning_mode};
}

sub get_accents_conf {
  my $mode=getconf("index_words_accent_mode");
  if ($mode eq "dual" || !defined $mode) {
    $unaccent=1;
    $add_unaccent=1;
  }
  elsif ($mode eq "strip") {
    $unaccent=1;
    $add_unaccent=0;
  }
  elsif ($mode eq "keep") {
    $unaccent=0;
    $add_unaccent=0;
  }
  $accents_configured=1;
}

sub load_stopwords {
  my $dbh=shift;
  %no_index_words = ();
  my $sth=$dbh->prepare("SELECT wordtext FROM non_indexable_words");
  $sth->execute;
  while (my @row=$sth->fetchrow_array) {
    $no_index_words{$row[0]}=1;
  }
  $sth->finish;
}

sub flush_word_vectors {
  my $dbh=shift;
  my $options=shift;

  my $sthu=$dbh->prepare("UPDATE inverted_word_index SET mailvec=?,nz_offset=? WHERE word_id=? AND part_no=?");

  my $vec_cnt_insert=0;
  my $vec_cnt_update=0;
  my $t0 = [gettimeofday];
  my @insert_array;

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
	$v2->Interval_Substitute($v, 0, 0, $nz_offset*8, ($v->Size()-$nz_offset*8));
	$bits=$v2->Block_Read();
	# Block_Read rounds to more than 8 bits (32 bits?) so we cut
	# to 8 bits
	$bits=substr($bits, 0, ($v2->Size()+7)/8);
      }
      else {
	$bits=$v->Block_Read();
	# Block_Read rounds to more than 8 bits (32 bits?) so we cut
	# to 8 bits
	$bits=substr($bits, 0, ($v->Size()+7)/8);
      }
      if (defined $vecs{$wid}->{$part}->{insert}) {
	#insert
	my $bits_text=$dbh->quote($bits, { pg_type=>DBD::Pg::PG_BYTEA });
	$bits_text =~ s/''/'/g;
	$bits_text = substr($bits_text,2,length($bits_text)-3);
	push @insert_array, [ $wid, $part, $bits_text, $nz_offset ];
	delete $vecs{$wid}->{$part}->{insert};
	$vec_cnt_insert++;
      }
      else {
	# update
	$sthu->bind_param(1, $bits, { pg_type => DBD::Pg::PG_BYTEA });
	$sthu->bind_param(2, $nz_offset);
	$sthu->bind_param(3, $wid);
	$sthu->bind_param(4, $part);
	$sthu->execute;
	$vec_cnt_update++;
      }
      # Can't do this check with indexes created with pre-1.3.0 versions
      # and not recreated.
      # if (length($bits)+$nz_offset>$partsize/8) {
      #	die sprintf("Vector too large (%d bytes) for (word_id,part_no)=(%d,%d)", length($bits)+$nz_offset, $wid, $part);
      #      }
      delete $vecs{$wid}->{$part}->{dirty};
    }
  }

  if (@insert_array>0) {
    $dbh->do("COPY inverted_word_index(word_id,part_no,mailvec,nz_offset) FROM STDIN");
    foreach my $vl (@insert_array) {
      $dbh->pg_putcopydata(join("\t", @{$vl})."\n");
    }
    $dbh->pg_putcopyend();
  }

  if (!defined $options->{no_jobs_queue}) {
    my $sthd=$dbh->prepare("DELETE FROM jobs_queue WHERE mail_id=? AND job_type='widx'");
    foreach (@flush_queue) {
      $sthd->execute($_);
    }
  }

  @flush_queue=();
  $last_flush_time=time;
  notice_log(sprintf("Index vectors flush: %d inserted, %d updated in %0.2fs",$vec_cnt_insert, $vec_cnt_update, tv_interval($t0)));
}

sub last_flush_time {
  return $last_flush_time;
}

sub clear_word_vectors {
  %vecs=();
  %hwords=();
  $vecs_estim_size=0;
  %cache_sth_w=();
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
  load_fti_config($dbh) if !defined($fti_config);
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

sub extract_words {
  my ($ptext, $tb)=@_;
  my %seen;
  foreach (split(/\n/, $$ptext)) {
    next if (length($_)>50 && !/[\s:\.\?,;]/);

    foreach (split(/[\x{0}-\x{1e}\x{80}-\x{bf}\s+,\.\(\)\\<\>\{\}\x{2000}-\x{206f}\"'`:;\/!\[\]\?=*\|]/o)) {
      next if (/^[-_#%|*=]+$/);  # skip horizontal separation lines
      if (/^[-~*_^|_=]+(.*)$/) {
	$_ = $1;
      }
      if (/^([^-~*^|_=]+)[-~*^|_=]+$/) {
	$_ = $1;
      }

      next if (length($_)<=2 || length($_)>50);
      $_=lc($_);
      next if (exists $seen{$_});
      $seen{$_}=1;
      next if (exists $no_index_words{$_});

      if ($unaccent && ! /^[0-9_a-z]+$/) {
	my $w=NFD($_);
	$w =~ s/\pM//g;  # strip combining characters
	if ($w ne $_) {
	  if (!exists $seen{$w} && !exists $no_index_words{$w}) {
	    # push the non-accented version
	    push @{$tb}, $w;
	    $seen{$w}=1;
	  }
	  push @{$tb}, $_ if ($add_unaccent);
	}
	else {
	  push @{$tb}, $_;
	}
      }
      else {
	push @{$tb}, $_;
      }
      # Add components of compound words
      my @cw=split /-/;
      if (@cw>1) {
	foreach (@cw) {
	  next if (length($_)<=2 || length($_)>50);
	  next if (exists $seen{$_});
	  $seen{$_}=1;
	  next if (exists $no_index_words{$_});
	  if ($unaccent && ! /^[0-9_a-z]+$/) {
	    my $w=NFD($_);
	    $w =~ s/\pM//g;	# strip combining characters
	    if ($w ne $_) {
	      if (!exists $seen{$w} && !exists $no_index_words{$w}) {
		# push the non-accented version
		push @{$tb}, $w;
		$seen{$w}=1;
	      }
	      push @{$tb}, $_ if ($add_unaccent);
	    } else {
	      push @{$tb}, $_;
	    }
	  } else {
	    push @{$tb}, $_;
	  }
	}
      }
    }
  }

  # extract complete email addresses, plus local part and domain
  # components, with and without TLD
  while ($$ptext =~ m/\b([A-Z0-9._%+-]+)\@([A-Z0-9.-]+)\.([A-Z]+)\b/gi) {
    my $lp=lc($1);
    my $h=lc($2);
    my $hh="$h.".lc($3);
    my $em=lc("$lp\@$hh");
    foreach ($em,$lp,$h,$hh) {
      next if (length($_)>50 || length($_)<=2);
      if (!exists $seen{$_}) {
	push @{$tb}, $_;
	$seen{$_}=1;
      }
    }
    # If the local part contains dots, extract parts
    foreach (split /\./, $lp) {
      next if (length($_)>50 || length($_)<=2);
      if (!exists $seen{$_}) {
	push @{$tb}, $_;
	$seen{$_}=1;
      }
    }
  }
}

sub words_table_name {
  my $c1=substr($_[0],0,1);
  my $c = ord($c1);
  if ($c>=97 && $c<=122) {
    return "words_$c1";
  }
  elsif ($c>=48 && $c<=57) {
    return "words_09";
  }
  else {
    return "words2";
  }
}

sub index_words {
  my ($dbh, $mail_id, $pbody, $pheader, $ref_more_text, $isub, $ctxt)=@_;
  load_fti_config($dbh) if !defined($fti_config);
  get_accents_conf() if (!$accents_configured);

  my $cnt=0;
  my $sth_w;
  my $sth_n;

  # The caller may tell us through $ctxt->{init_empty} that the partition
  # in inverted_word_index is empty. In this case, we don't SELECT from it.
  # This is a significant optimization when mass-reindexing.
  my $part_is_empty = $ctxt->{init_empty};

  if (!$words_partitioning_mode) {
    $sth_w = $dbh->prepare("SELECT word_id FROM words WHERE wordtext=?");
    $sth_n = $dbh->prepare("INSERT INTO words(word_id,wordtext) VALUES (nextval('seq_word_id'),?) RETURNING word_id");
  }

  my $svec = $dbh->prepare("SELECT mailvec,nz_offset FROM inverted_word_index WHERE word_id=? AND part_no=?");

  my @words;
  extract_words($pbody, \@words);
  extract_words($pheader, \@words) if (defined $pheader);
  extract_words($ref_more_text, \@words) if (defined $ref_more_text);

  # hbody_words contains a unique entry for each word occurring in the
  # body, and is used to avoid inserting multiple (word_id,mail_id) tuples.
  my %hbody_words;

  for my $s (@words) {
    next if (defined($no_index_words{$s}) or defined($hbody_words{$s}));
    $hbody_words{$s}=1;

    # Find the word_id
    my $word_id=$hwords{$s};
    if (!defined $word_id) {
      # The word hasn't been encountered before in any mail
      my $se=encode_dbtxt($s);
      my @r;

      if ($words_partitioning_mode) {
	my $words_table = words_table_name($se);
	$sth_w = $cache_sth_w{$words_table};
	if (!defined $sth_w) {
	  $sth_w = $dbh->prepare("SELECT word_id FROM $words_table WHERE wordtext=?");
	  $cache_sth_w{$words_table} = $sth_w;
	}
	$sth_w->execute($se);
	@r=$sth_w->fetchrow_array;
      }
      else {
	$sth_w->execute($se);
	@r=$sth_w->fetchrow_array;
      }

      if (@r) {
	$word_id=$r[0];
	$hwords{$s}=$word_id;
      }
      else {
	if (defined $isub) {
	  # The word isn't in the words table and we want it from our parent
	  $word_id = $isub->($se, $ctxt);	
	  if (!$word_id) {
	    die "Failed to obtain a word_id for '$se' from external function";
	  }
	  $hwords{$s}=$word_id;
	}
	else {
	  # The word isn't in the words table and we want to insert it directly
	  if ($words_partitioning_mode) {
	    my $words_table = words_table_name($se);
	    $sth_n = $dbh->prepare("INSERT INTO $words_table(word_id,wordtext) VALUES (nextval('seq_word_id'),?) RETURNING word_id", {pg_server_prepare=>0});
	    $sth_n->execute($se);
	    ($word_id) = $sth_n->fetchrow_array;
	  }
	  else {
	    $sth_n->execute($se);
	    ($word_id) = $sth_n->fetchrow_array;
	  }
	  if (!$word_id) {
	    die "Failed to obtain a word_id for '$se' from database";
	  }
	  $hwords{$s}=$word_id;
	}
      }
    }

    # Find the vector
    my $part_no = $mail_id / $partsize;
    my $bit_id = ($mail_id-1) % $partsize;
    my $vec = $vecs{$word_id}->{$part_no}->{v};

    if (!defined $vec) {
      $svec->execute($word_id, $part_no) unless $part_is_empty;
      if (!$part_is_empty && $svec->rows>0) {
	# found in db
	my @r=$svec->fetchrow_array;
	my $bits = "\000"x$r[1] . $r[0];
	$vec = Bit::Vector->new(length($bits)*8);
	# Can't do this check with indexes created with pre-1.3.0 versions
	# and not recreated.
	# if (length($bits) > $partsize/8)	{ # sanity check
	#  die sprintf("Word vector read from database for (word_id=%d,part_no=%d) exceeds the maximum size (%d bytes,max=%d bytes)", $word_id, $part_no, length($bits), $partsize/8);
	# }
	$vecs{$word_id}->{$part_no}->{v} = $vec;
	$vec->Block_Store($bits);
	$vecs_estim_size += ($vec->Size()+7)>>3;
      }
      else {
	$vec = Bit::Vector->new($bit_id+1);
	$vecs{$word_id}->{$part_no}->{v} = $vec;
	$vecs{$word_id}->{$part_no}->{insert} = 1; # not in db yet
	$vecs{$word_id}->{$part_no}->{dirty} = 1;
      }
    }
    if ($vec->Size()<$bit_id+1) {
      $vecs_estim_size += scalar(($bit_id+1+7>>3)-($vec->Size()+7>>3));
      $vec->Resize($bit_id+1);
    }
    if (!$vec->bit_test($bit_id)) {
      $vec->Bit_On($bit_id);
      $vecs{$word_id}->{$part_no}->{dirty} = 1;
    }
    $cnt++;
  }

  if ($cnt) {
    push @flush_queue, $mail_id;
  }
  if ($mail_id > $max_mail_id_index_flush) {
    $max_mail_id_index_flush = $mail_id;
  }
}

sub fetch_vec {
  my ($sth_w, $svec, $vr, $w)=@_;

  $sth_w->execute($w);
  my ($wid)=$sth_w->fetchrow_array;
  return 0 if (!$wid);
  $svec->execute($wid);
  while (my ($vecbits,$part,$nz_offset) = $svec->fetchrow_array) {
    my $bits = "\000"x$nz_offset . $vecbits;
    my $v = Bit::Vector->new(length($bits)*8);
    $v->Block_Store($bits);
    if (defined $vr->{$part}) {
      if ($vr->{$part}->Size() < $v->Size()) {
	$v->Resize($vr->{$part}->Size());
      }
      elsif ($vr->{$part}->Size() > $v->Size()) {
	$vr->{$part}->Resize($v->Size());
      }
      $vr->{$part}->And($vr->{$part}, $v);
    }
    else {
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
  load_fti_config($dbh) if !defined($fti_config);
  my ($nb_substrings, $nb_words);
  my $sth_w = $dbh->prepare("SELECT word_id FROM words WHERE wordtext=?");
  my $svec = $dbh->prepare("SELECT mailvec,part_no,nz_offset FROM inverted_word_index WHERE word_id=?");

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
	my $s=$dbh->prepare("SELECT mail_id FROM body WHERE bodytext ilike $qw AND mail_id IN ($mid) ORDER BY mail_id");
	$s->execute;
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
sub header_contents_to_ftidx {
  my $r;
  while ($_[0] =~ /^(Subject|From|To|Cc|Date): (.*)$/img) {
    $r.="$2\n";
  }
  return $r;
}


sub flush_jobs_queue {
  my $dbh=shift;
  my $sth=$dbh->prepare("SELECT job_id,mail_id FROM jobs_queue WHERE job_type='widx'");
  $sth->execute;
  if ($sth->rows>0) {
    my %extractors = Manitou::Attachments::text_extractors();
    my $idx_html = getconf_bool("index_words_html_parts");

    my $col_html = $idx_html ? "bodyhtml":"null";
    my $sthb=$dbh->prepare("SELECT bodytext,$col_html FROM body WHERE mail_id=?");
    my $sthh=$dbh->prepare("SELECT lines FROM header WHERE mail_id=?");
    while (my ($job_id,$mail_id)=$sth->fetchrow_array) {
      $sthb->execute($mail_id) or die $dbh->errstr;
      my ($body,$html)=$sthb->fetchrow_array;
      $sthh->execute($mail_id) or die $dbh->errstr;
      my ($header)=$sthh->fetchrow_array;
      $body = decode_dbtxt($body);
      my $other_words;
      if ($html) {
	$html = decode_dbtxt($html);
	$other_words = html_to_text(\$html);
      }
      $header = decode_dbtxt($header);
      $header = Manitou::Words::header_contents_to_ftidx($header);
      if (scalar(%extractors)!=0 || ($idx_html && length($html)>0)) {
	Manitou::Attachments::launch_text_extractors($dbh, $mail_id,
						     \%extractors,
						     \$other_words);
      }
      index_words($dbh, $mail_id, \$body, \$header, \$other_words);
    }
  }

  if (queue_size()>0) {
    flush_word_vectors($dbh);
    clear_word_vectors;
  }
}

# Taken from HTML::Element::as_text and modified to add a space
# after each piece of text
sub in_html_to_text {
  # Yet another iteratively implemented traverser
  my($this,%options) = @_;
  my $nillio = [];
  my(@pile) = ($this);
  my $tag;
  my $text = '';

  while(@pile) {
    if(!defined($pile[0])) { # undef!
      # no-op
    } elsif(!ref($pile[0])) { # text bit!  save it!
      $text .= (shift @pile)." ";
    } else { # it's a ref -- traverse under it
      unshift @pile, @{$this->{'_content'} || $nillio}
        unless
          ($tag = ($this = shift @pile)->{'_tag'}) eq 'style'
          or $tag eq 'script';
    }
  }
  return $text;
}

# In: html source
sub html_to_text {
  my $tree = HTML::TreeBuilder->new;
  $tree->store_declarations(0);
  eval {
    $tree->parse_content($_[0]);
  };
  my $txt= $@ ? undef:in_html_to_text($tree);
  $tree->delete();
  return $txt;
}

1;
