# TNEF attachments decoder plugin for Manitou-Mail
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

package Manitou::Plugins::tnef_decoder;

use Convert::TNEF;
use MIME::Types;
use Manitou::Config qw(getconf);


sub init {
  my $self={};
  bless $self;
  $self->{mime_types} = MIME::Types->new;
  return $self;
}

sub finish {
 1;
}

sub process {
  my ($self,$ctxt)=@_;
  my $obj=$ctxt->{'mimeobj'};
  if ($obj->is_multipart) {
    foreach my $subobj ($obj->parts) {
      if ($subobj->effective_type =~/ms-tnef/i) {
	my $tnef = Convert::TNEF->read_ent($subobj,
					   {output_dir=>getconf("tmpdir")})
	  or die $Convert::TNEF::errstr;

	for my $a ($tnef->attachments) {
	  my $type;
	  if ($a->longname=~/\.([A-Za-z]{2,4})$/) {
	    $type = $self->{mime_types}->mimeTypeOf($1);
	  }
	  if (!$type) {
	    $type="application/octet-stream";
	  }

	  $obj->attach(Filename => $a->longname,
		       Type => $type,
		       Data => $a->data,
		       Disposition => "attachment");
	}
	$tnef->purge;
      }
    }
    # Remove the ms-tnef parts
    $obj->parts([ grep { !($_->effective_type =~/ms-tnef/i) } $obj->parts ]);
  }
}

1;
