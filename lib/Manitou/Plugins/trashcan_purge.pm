# Trashcan auto-emptying plugin for manitou-mail
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

package Manitou::Plugins::trashcan_purge;

use DBI;

sub init {
  shift;			# dbh
  my ($args)=@_;
  my $self={};
  bless $self;
  $self->{'delay_days'} = $_[0] || 30;
  return $self;
}
sub finish {
  1;
}

sub process {
  my $self=$_[0];
  return 1 if (!$self->{'delay_days'});	# shouldn't happen

  my $dbh=$_[1]->{dbh};
  local $dbh->{RaiseError}=1;
  local $dbh->{PrintError}=1;
  eval {
    $dbh->begin_work;
    my $q=$dbh->prepare("SELECT mail_id FROM mail WHERE status&16=16 AND msg_date<now()-?::interval");
    $q->execute($self->{'delay_days'} . ' days');
    my $d=$dbh->prepare("SELECT delete_msg(?)");
    while (my ($mail_id)=$q->fetchrow_array) {
      $d->execute($mail_id);
    }
  };
  if ($@) {
    # An error has occurred. The error message should have been printed
    # to STDERR already
    $dbh->rollback;   # clean up the transaction
  }
  else {
    $dbh->commit;
  }
}

1;
