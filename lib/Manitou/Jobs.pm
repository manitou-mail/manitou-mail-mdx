# Copyright (C) 2004-2017 Daniel Verite

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



# Process asynchronous (forked) jobs

package Manitou::Jobs;

use strict;
use vars qw(@ISA @EXPORT_OK);

use Manitou::Mailing;
use POSIX ":sys_wait_h";

require Exporter;
@ISA = qw(Exporter);
#@EXPORT_OK = qw();

use Manitou::Mailing;

my %active_pids;

sub process_async_jobs_queue {
  my $dbh=shift;
  my $sth=$dbh->prepare("SELECT job_id,job_type,job_args FROM jobs_queue WHERE job_type='mailing'");
  $sth->execute;
  while (my $row=$sth->fetchrow_hashref) {
    next if (defined $active_pids{$row->{job_id}}); # already running
    if ($row->{job_type} eq 'mailing') {
      my $pid = Manitou::Mailing::async_process_mailing($dbh, $row->{job_args});
      $active_pids{$row->{job_id}} = $pid;
    }
  }
}

sub check_end_async_jobs {
  my $dbh=shift;
  if (scalar(%active_pids)>0) {
    my $pid;
    do {
      $pid = waitpid(-1, WNOHANG);
      if ($pid>0) {
	for my $id (keys %active_pids) {
	  if ($active_pids{$id} == $pid) {
	    my $s=$dbh->prepare("DELETE FROM jobs_queue WHERE job_id=?");
	    $s->execute($id);
	    delete $active_pids{$id};
	    last;
	  }
	}
      }
    } while ($pid>0);
  }
}

1;
