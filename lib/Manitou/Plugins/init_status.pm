# init_status plugin for Manitou-Mail
# Set the initial status of messages.
# Declare as a plugin in manitou-mdx.conf. Example:
# incoming_mimeprocess_plugins = init_status({status=>33});

# Copyright (C) 2016 Daniel Verite

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

package Manitou::Plugins::init_status;

use Data::Dumper;

sub init {
  shift;			# dbh
  my ($args)=@_;
  my $self= bless {};
  $self->{args} = $_[0];

  $self;
}

sub finish {
  # nothing to do
  1;
}

sub process {
  my ($self,$ctxt)=@_;
  # Set the mail status to archived+read if not already set.
  print "status=", $ctxt->{status};
  print Dumper($self);
  if ($ctxt->{status} == 0 && defined $self->{args}->{status}) {
    $ctxt->{status} = $self->{args}->{status};
  }
  1;
}

1;
