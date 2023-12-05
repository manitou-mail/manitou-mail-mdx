# Outgoing header plugin for Manitou-Mail
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

########################################################################
# This plugin is a skeleton that shows how to modify the header
# of an outgoing message before it gets passed to the local mailer
########################################################################

package Manitou::Plugins::outgoing_header;

use strict;

sub init {
  my $self=bless {};
  # Other initializations
  return $self;
}

sub finish {
  1;
}

sub process {
  my ($self,$ctxt)=@_;
  my $top=$ctxt->{mimeobj};
  my ($h,$v)=("Organization", "My Company"); # New field or new value for an existing field
  $top->head->replace($h,$v);
  # save the change in the database as well (optional)
  $ctxt->{dbh}->do("SELECT replace_header_field(?,?,?)", undef, $ctxt->{mail_id}, $h, $v);

  1;
};

1;
