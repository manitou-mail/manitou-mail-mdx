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

use strict;

package Manitou::Tags;

require Exporter;
use vars qw(@ISA @EXPORT_OK);
use Carp;
use Manitou::Encoding qw(decode_dbtxt encode_dbtxt);

@ISA = qw(Exporter);
@EXPORT_OK = qw(action_tag flattened_tag_name);

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

sub action_tag {
  my ($dbh, $mail_id, $tag_id) = @_;
  my $sth=$dbh->prepare("INSERT INTO mail_tags(mail_id,tag) SELECT ?,? WHERE NOT EXISTS (SELECT 1 FROM mail_tags WHERE mail_id=? AND tag=?)");
  $sth->execute($mail_id, $tag_id, $mail_id, $tag_id);
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
    $ht->{@{$t{$id}}[2]}=$id;   # keep only the full hierarchical name
    $htuc->{uc(@{$t{$id}}[2])}=$id;   # keep only the full hierarchical name
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

sub insert_tag {
  my ($dbh, $mail_id, $tagname) = @_;
  my %tags;
  my %uc_tags;
  load_tags($dbh, \%tags, \%uc_tags);
  my $tag_id=$uc_tags{uc($tagname)};
  if (!$tag_id) {
    $tag_id=create_tag_hierarchy($dbh, $tagname, \%uc_tags);
  }
  if ($tag_id) {
    action_tag($dbh, $mail_id, $tag_id);
  }
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

1;
