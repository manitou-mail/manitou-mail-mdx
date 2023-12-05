# Copyright (C) 2004-2023 Daniel Verite

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

# overwrite_mime_type is a mimeprocess plugin that is meant to
# overwrite the "application/octet-stream" MIME types of
# top-level MIME parts by using File::LibMagic to determine
# the type based on the contents of the file.
# See https://metacpan.org/pod/File::LibMagic

package Manitou::Plugins::overwrite_mime_type;

use File::LibMagic;
use strict;

sub init {
  bless {}
}
sub finish {
  1;
}

sub process {
  my ($self, $context) = @_;
  return 1 if (!defined $context->{mimeobj});
  my $top = $context->{mimeobj};

  if ($top->is_multipart) {
    for my $obj ($top->parts) {
      if ($obj->mime_type eq "application/octet-stream" && defined $obj->bodyhandle) {
	my $magic = File::LibMagic->new();
	my $io = $obj->bodyhandle->open("r");
	my $info = $magic->info_from_handle($io);
	if (defined $info->{mime_type}) {
	  # $context->{notice_log}("overwriting mime type with " . $info->{mime_type});
	  $obj->head->replace("content-type", $info->{mime_type});
	}
      }
    }
  }
  1;
}

1;
