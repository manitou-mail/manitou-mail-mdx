# Spamassassin plugin for Manitou-Mail to detect spamness of an incoming message
# by calling 'spamc' (spamassassin's client program)

# Copyright (C) 2010 Daniel Verite
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


### HOW TO USE ###
# Declare the plugin as an incoming preprocess plugin in manitou-mdx
# configuration file.

# Examples:
# To automatically tag the messages with a 'spam' tag
# incoming_preprocess_plugins = spamc({tag=>"spam"})

# To automatically tag the spam messages and move them immediately to the trashcan:
# incoming_preprocess_plugins = spamc({tag=>"spam", trash=>1})

# List of arguments (all optional):
# tag => name of tag
# trash => 1 (or any non-zero value. A zero value is equivalent to not having the entry)
# status => "trashed". Only for compatibility with the previous version. Use "trash"=>1 instead.
# notice => 1 (or any non-zero value). Report any detected spam to the caller through the notification system (typically generates a syslog entry)
###

package Manitou::Plugins::spamc;

sub init {
  my $dbh=shift;
  my ($args)=@_;
  my $self={};
  bless $self;

  # The plugin argument can be either a string (name of a tag)
  # or a reference to a hash: { tag=>name of tag, status=>trashed }
  if (defined $args) {
    if (ref($args) eq "HASH") {
      $self->{spamtag}=$args->{tag} if (defined $args->{tag});
      $self->{status}="trashed" if ($args->{status} eq "trashed" || $args->{trash});
      $self->{notice} = $args->{notice};
    }
    else { $self->{spamtag}=$args; }
  }
  return $self;
}

sub finish {
  # nothing to do
  1;
}

sub process {
  my ($self,$ctxt)=@_;
  if ((defined $self->{spamtag} || defined $self->{status})
      && defined($ctxt->{filename})) {
    my $r=`spamc -c < $ctxt->{filename}`;
    if ($?==256 && $r =~ /\//) {	# if spamc says the message is spam
      $ctxt->{notice_log}("spamc plugin says $ctxt->{filename} is a spam ($r)") if ($self->{notice});
      if (defined $self->{spamtag}) {
	push @{$ctxt->{tags}}, $self->{spamtag}; # then add the relevant tag
      }
      if ($self->{status} eq "trashed") {
	$ctxt->{status} |= 16+32;  # trashed+processed
      }
    }
  }
  1;
}

1;
