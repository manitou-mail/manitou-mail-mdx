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

use strict;

package Manitou::Tags;

require Exporter;
use vars qw(@ISA @EXPORT_OK);
use Carp;
use Manitou::Encoding qw(decode_dbtxt encode_dbtxt);
use Manitou::Config qw(getconf getconf_bool);

@ISA = qw(Exporter);
@EXPORT_OK = qw(assign_tags flattened_tag_name consolidate_tags_counts);

sub get_sequence_nextval {
  my ($dbh, $seq) = @_;
  my ($nextval, $sth, $row);
  $sth=$dbh->prepare("SELECT nextval('".$seq."')") or croak $dbh->errstr;
  $sth->execute() or croak $dbh->errstr;
  my @row = $sth->fetchrow_array;
  if ($row[0]) {
    $nextval = $row[0];
  } else {
    $nextval = 1;
  }
  $sth->finish;
  return $nextval;
}

# Input: dbh, mail_id, list of tag_id
sub assign_tags {
  my $dbh = shift;
  my $mail_id = shift;
  return if (!@_);  # empty list

  # deduplicate tag IDs, sort them, and format as a VALUES list
  my %seen;
  my @arr = sort { $a <=> $b } grep !$seen{$_}++, @_;
  # my $values = join ",", map { "($_)" } @arr;
  my $tags_array = "{" . (join ",", @arr) . "}";

  my $sth = $dbh->prepare(q{
     WITH res(t) AS
      (INSERT INTO mail_tags(mail_id,tag)
        SELECT ?,t FROM unnest(?::int[]) AS s(t)
         WHERE NOT EXISTS (SELECT 1 FROM mail_tags WHERE mail_id=? AND tag=t)
       RETURNING tag)
     SELECT array_agg(t) FROM res
  });
  $sth->execute($mail_id, $tags_array, $mail_id);
  my @r = $sth->fetchrow_array;
  $sth->finish;
  if (defined $r[0] && getconf_bool("materialize_tags_counts")) {
    # update main counters
    my $sth1 = $dbh->prepare("UPDATE tags_counters SET cnt=cnt+1 WHERE temp=false AND tag_id=ANY(?::int[]) AND EXISTS (SELECT 1 FROM mail WHERE mail_id=? AND status&(32+16)=32)");
    $sth1->execute($r[0], $mail_id);
  }
}


sub load_tags {
  my ($dbh, $ht, $htuc)=@_;			# name=>id
  my %t;
  my $sth=$dbh->prepare("SELECT tag_id,name,parent_id FROM tags");
  $sth->execute;
  my @r;
  while (@r=$sth->fetchrow_array) {
    $r[1] = decode_dbtxt($r[1]);
    $t{$r[0]}= [$r[1], $r[2], $r[1]]; # (name, parent, name_with_hierarchy)
  }
  $sth->finish;
  foreach my $id (keys %t) {
    my $parent_id = @{$t{$id}}[1];
    while ($parent_id) {
      @{$t{$id}}[2] = @{$t{$parent_id}}[0] . "->" . @{$t{$id}}[2];
      $parent_id = @{$t{$parent_id}}[1];
    }
    $ht->{@{$t{$id}}[2]} = $id;   # keep only the full hierarchical name
    $htuc->{uc(@{$t{$id}}[2])} = $id;   # keep only the full hierarchical name
  }
}

sub new_tag {
  my ($dbh, $parent_id, $name)=@_;
  my $tag_id=get_sequence_nextval($dbh, "seq_tag_id");
  return if (!$tag_id);
  my $sth2=$dbh->prepare("INSERT INTO tags(tag_id,name,parent_id) VALUES (?,?,?)");
  if ($sth2->execute($tag_id, encode_dbtxt($name), $parent_id)) {
    return $tag_id;
  }
  else {
    croak $dbh->errstr;
    return undef;
  }
}

# $_[1] is a tag's name in tree-like representation
# Split it into a tag hierarchy and create the parts of the hierarchy
# that don't already exist.
sub create_tag_hierarchy {
  my ($dbh, $name, $t)=@_;
  my @names= split /\-\>/, $name;
  if (@names>0) {
    my $parent_id;
    my $hierch_name;
    my $id;
    for my $n (@names) {
      if ($hierch_name ne "") {
	$hierch_name .= "->";
      }
      $hierch_name .= uc($n);
      if (defined($t->{$hierch_name})) {
	$parent_id=$t->{$hierch_name};
      }
      else {
	$id=new_tag($dbh, $parent_id, $n);
	$t->{$hierch_name} = $id;
	$parent_id=$id;
      }
    }
    return $id;
  }
  else {
    return new_tag($dbh, undef, $name);
  }
}

# Insert tags from a list of hierarchical names.
# Duplicate names are permitted.
# Possibly create the tags if some don't exist.
sub insert_tags {
  my ($dbh, $mail_id, @tagnames) = @_;
  my %tags;
  my %uc_tags;
  load_tags($dbh, \%tags, \%uc_tags);
  my @tag_ids;
  for my $tagname (@tagnames) {
    my $tag_id = $uc_tags{uc($tagname)};
    if (!$tag_id) {
      $tag_id = create_tag_hierarchy($dbh, $tagname, \%uc_tags);
    }
    push @tag_ids, $tag_id;
  }
  assign_tags($dbh, $mail_id, @tag_ids);
}

# Returns the tag's full hierarchical name from its tag_id
sub flattened_tag_name {
  my ($dbh,$tag_id)=@_;
  my @names;
  my $sth=$dbh->prepare("SELECT parent_id,name FROM tags WHERE tag_id=?");
  while ($tag_id) {
    $sth->execute($tag_id);
    my $namepart;
    ($tag_id,$namepart) = $sth->fetchrow_array;
    push @names, decode_dbtxt($namepart);
  }
  return join("->", reverse @names);
}


# Return a tag_id from a full hierarchical name
sub tag_id_from_name {
  my ($dbh, $tagname) = @_;
  my %tags;
  my %uc_tags;
  load_tags($dbh, \%tags, \%uc_tags);
  return $uc_tags{uc($tagname)};
}

sub consolidate_tags_counts {
  my $dbh = shift;
  local $dbh->{AutoCommit}=1;

  # Sum and delete the temporary counts (temp=true), transferring them
  # into the permanent counts (temp=false)
  # Permanent counts should exist for all tags, otherwise temporary
  # entries without a permanent counterpart are lost.

  $dbh->do(qq{WITH d as
    (delete from tags_counters where temp=true returning tag_id, cnt)
      UPDATE tags_counters set cnt=cnt+s from
        (select tag_id,sum(cnt) as s from d group by tag_id) sums
      WHERE sums.tag_id=tags_counters.tag_id and temp=false
    });

}

1;
