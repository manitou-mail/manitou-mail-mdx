# pdf attachments indexing plugin for manitou-mail
# Copyright (C) 2005-2019 Daniel Verite
# Copyright (C) 2008 Luis Amigo

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

package Manitou::Plugins::pdfindexer;

use File::Temp;

# The output of pdftotext is to be interpreted as utf8, and then
# possibly converted back into the database encoding
use open IN => ':utf8';

sub init {
  shift;			# dbh
  my ($args)=@_;
  my $self={};
  bless $self;
  $self->{'command'} = $_[0] || "pdftotext -q -nopgbrk";
  return $self;
}

sub finish {
  # nothing to do
  1;
}

sub index_file {
  my ($fh, $ctxt)=@_;
  my $text;
  {
    local $/;
    $text=<$fh>;
  }
  if (defined $text) {
    Manitou::Words::index_words($ctxt->{'dbh'}, $ctxt->{'mail_id'}, \$text);
    Manitou::Words::flush_word_vectors($ctxt->{'dbh'});
  }
}

sub process {
  my ($self,$ctxt)=@_;
  my $obj=$ctxt->{'mimeobj'};
  if ($obj->is_multipart) {
    foreach my $subobj ($obj->parts) {
      my $type=$subobj->effective_type;
      if ($type eq "application/pdf") {
	my $tmpfh = File::Temp->new(UNLINK=>0) or die "Cannot create a temporary file\n";
	my $fname = $tmpfh->filename;
	binmode $tmpfh;
	$subobj->bodyhandle->print($tmpfh);
	close($tmpfh);
	# run wvWare
	if (!open(F2, $self->{'command'} . " $fname - |")) {
	  unlink($fname);
	  print STDERR "Unable to execute $self->{'command'} $fname\n";
	  die $!;
	}
	index_file(\*F2, $ctxt);
	close(F2);
	unlink($fname);
      }
    }
  }
  1;
}

1;
