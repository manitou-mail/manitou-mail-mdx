# HTML attachments indexer plugin for Manitou-Mail
# Copyright (C) 2009 Daniel Verite

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

package Manitou::Plugins::html_indexer;

use HTML::TreeBuilder;
use HTML::FormatText;
use IO::Handle;

sub init {
  shift;			# dbh
  my ($args)=@_;
  my $self={};
  bless $self;
  return $self;
}

sub finish {
  # nothing to do
  1;
}

sub index_contents {
  my ($fh, $ctxt)=@_;
  my $html;
  my $text;
  {
    local $/;
    $html = $fh->getline();
  }
  
  if (defined $html) {
    my $tree = HTML::TreeBuilder->new;
    $tree->parse_content($html);
    my $formatter = HTML::FormatText->new(leftmargin=>0, rightmargin=>78);
    $text = $formatter->format($tree);
  }
  if (defined $text) {
    Manitou::Words::index_words($ctxt->{'dbh'}, $ctxt->{'mail_id'}, \$text);
  }
}

sub process_parts {
  my ($obj,$ctxt) = @_;;
  if ($obj->is_multipart) {
    foreach my $subobj ($obj->parts) {
      process_parts($subobj, $ctxt);	# recurse
    }
  }
  else {
    my $type=$obj->effective_type;
    if ($type eq "text/html") {
      my $io = $obj->bodyhandle->open("r");
      index_contents($io, $ctxt);
      $io->close;
    }
  }
}

sub process {
  my ($self,$ctxt)=@_;
  process_parts($ctxt->{'mimeobj'}, $ctxt);
  1;
}

1;
