# Bayesian-based automated classification plugin
# Copyright (C) 2005 Daniel Vérité

   # The implementation of the classification algorithm originally
   # comes from POPFile v0.22 (file: Classifier/Bayes.pm) distributed
   # under the GPL version 2 or later
   # Copyright (c) 2001-2004 John Graham-Cumming
   # Hosted at http://popfile.sourceforge.net

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

package Manitou::Plugins::bayes_classify;

use strict;
use Manitou::Tags qw(flattened_tag_name);

use DBI;
my $debug=0;

sub init {
  return bless {};
}

sub finish {
  1;
}

sub classify {
  my ($self, $pbody, $dbh,$analyze)=@_;
  my @words=split(/\W/, $$pbody);
  return undef if (@words==0);

  my $sth_w = $dbh->prepare("SELECT word_id FROM words WHERE wordtext=?");
  my $stht = $dbh->prepare("SELECT tag_id, counter FROM tags_words WHERE word_id=?");

  my %tags_count;
  my $total_word_count=0;

  my $sthrt=$dbh->prepare("SELECT rt_value FROM runtime_info WHERE rt_key='last_bayesian_learning'");
  $sthrt->execute;
  my ($rt_value) = $sthrt->fetchrow_array;

  if (!defined($rt_value) || !defined($self->{last_learn}) || $rt_value!=$self->{last_learn}) {
    # Compute the number of entries per tag.
    # The query is slow, so we cache the results and invalidate
    # the cache only when runtime_info(rt_key='last_bayesian_learning')
    # tells us the tags_words table has changed
    my $sthc=$dbh->prepare("SELECT tag_id,count(*) FROM tags_words GROUP BY tag_id");
    $sthc->execute;
    $self->{tags_count_cache} = ();
    while (my @r=$sthc->fetchrow_array) {
      $tags_count{$r[0]}=$r[1];
      $total_word_count += $r[1];
      $self->{tags_count_cache}{$r[0]} = $r[1];
    }

    my $sthu_rt=$dbh->prepare("UPDATE runtime_info SET rt_value=? WHERE rt_key='last_bayesian_learning'");
    my $t=time;
    $self->{last_learn} = $t;
    $sthu_rt->execute($t);
    if ($sthu_rt->rows==0) {
      $sthu_rt=$dbh->prepare("INSERT INTO runtime_info(rt_key,rt_value) VALUES('last_bayesian_learning', ?)");
      $sthu_rt->execute($t);
    }
  }
  else {
    foreach my $k (keys %{$self->{tags_count_cache}}) {
      $tags_count{$k}=$self->{tags_count_cache}{$k};
      $total_word_count += $self->{tags_count_cache}{$k};
    }
  }

  return undef if ($total_word_count==0);

  my %score;
  my %matchcount;
  foreach my $t (keys %tags_count) {
    $matchcount{$t}=0;
    $score{$t} = log($tags_count{$t} / $total_word_count);
  }
  my $not_likely = -log(10 * $total_word_count);

  my %hwords;
  my %matrix;
  my %tags;
  my %word_counts;		# word_id=>number of occurrences in text
  for my $s (@words) {
    next if (length($s)<=2 || length($s)>30);
    next if ($s =~ /^[0-9]+$/);
    $s = lc($s);
    my $word_id=$hwords{$s};
    if (!defined($word_id)) {
      $sth_w->execute ($s);
      my @r=$sth_w->fetchrow_array;
      if (@r) {
	$word_id=$r[0];
	$hwords{$s}=$word_id;
	$word_counts{$word_id}++;
	$stht->execute($word_id);
	while (my @r1=$stht->fetchrow_array) {
	  $matrix{$word_id}{$r1[0]}=$r1[1];
	  $tags{$r1[0]}=1;
	}
      }
      # words missing in database are ignored
    }
    else {
      $word_counts{$word_id}++;
    }
  }

  my $correction=0;
  foreach my $id (keys %word_counts) {
    my $wmax=-10000;
    foreach my $bucket (keys %tags) {
      my $probability=0;
      if (defined($matrix{$id}{$bucket}) && $matrix{$id}{$bucket}>0) {
	$probability = log($matrix{$id}{$bucket} / $tags_count{$bucket});
      }
      if ($probability!=0) {
	$matchcount{$bucket} += $word_counts{$id};
      }
      else {
	$probability = $not_likely;
      }
      $wmax = $probability if ( $wmax < $probability );
      $score{$bucket} += ($probability * $word_counts{$id});
    }

    if ($wmax > $not_likely) {
      $correction += $not_likely * $word_counts{$id};
    }
    else {
      $correction += $wmax * $word_counts{$id};
    }
  }

  # Now sort the scores to find the highest and return that bucket
  # as the classification

  my @ranking = sort {$score{$b} <=> $score{$a}} keys %score;

  my %raw_score;
  my $base_score = $score{$ranking[0]};
  my $total = 0;

  # If the first and second bucket are too close in their
  # probabilities, call the message unclassified.  Also if there are
  # fewer than 2 buckets.

  my $unclassified = log(100);	# unclassified_weight

  my $result_tag;

  if (scalar(%tags_count) > 1 &&
      $score{$ranking[0]} > ($score{$ranking[1]} + $unclassified)) {
    $result_tag = $ranking[0];
  }

  # Optional
  # Compute the total of all the scores to generate the normalized
  # scores and probability estimate.  $total is always 1 after the
  # first loop iteration, so any additional term less than 2 ** -54
  # is insignificant, and need not be computed.

  if ($analyze) {
    my $ln2p_54 = -54 * log(2);
    foreach my $b (@ranking) {
      $raw_score{$b} = $score{$b};
      $score{$b} -= $base_score;

      $total += exp($score{$b}) if ($score{$b} > $ln2p_54 );
    }

    my $log10 = log(10.0);
    foreach my $b (@ranking) {
      my $prob = exp($score{$b})/$total;
      my $probstr;
      my $rawstr;

      # If the computed probability would display as 1, display
      # it as .999999 instead.
      if ($prob >= .999999) {
	$probstr = sprintf("%12.6f", 0.999999);
      }
      elsif ($prob >= 0.1 || $prob == 0.0) {
	$probstr = sprintf("%12.6f", $prob);
      }
      else {
	$probstr = sprintf("%17.6e", $prob);
      }

      $rawstr = sprintf("%12.6f", ($raw_score{$b} - $correction)/$log10);
      my $tn=flattened_tag_name($b);
      print "tag: $tn, matchcount=$matchcount{$b} $probstr, $rawstr\n";
    }
  }

  return $result_tag;
}

sub process {
  my ($self,$ctxt)=@_;
  my $top=$ctxt->{mimeobj};
  my ($body_text, $charset);
  if (!defined($top->effective_type) || $top->effective_type eq "text/plain") {
    $body_text=$top->bodyhandle->as_string unless (!$top->bodyhandle);
    $charset = $top->head->mime_attr("content-type.charset");
  }
  elsif ($top->is_multipart) {
    # if no body, let's search for a text/plain part at the level 1 of
    # the MIME tree
    foreach ($top->parts) {
      if ($_->bodyhandle && $_->effective_type eq "text/plain") {
	$body_text = $_->bodyhandle->as_string;
	$charset = $_->head->mime_attr("content-type.charset");
	last;
      }
    }
  }
  if (defined($body_text)) {
    $charset='iso-8859-15' if (!defined($charset) || !Encode::resolve_alias($charset));
    $body_text = Encode::decode($charset, $body_text);
    my $tag_id=classify($self, \$body_text, $ctxt->{dbh});
    if (defined($tag_id)) {
      push @{$ctxt->{tags}}, flattened_tag_name($ctxt->{dbh}, $tag_id);
    }
  }
  1;
}

1;
