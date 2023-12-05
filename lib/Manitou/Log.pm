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

package Manitou::Log;

use Sys::Syslog;

use strict;
use vars qw(@ISA @EXPORT_OK);

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(notice_log error_log init_log debug_log warning_log);

my $dest;

sub init_log {
  $dest=shift;
  if ($dest eq "syslog") {
    openlog("manitou-mdx", "pid", "user");
  }
}

sub notice_log {
  my $msg=shift;
  if ($dest eq "syslog") {
    syslog("notice", $msg);
  }
  else {
    print STDERR "Notice: $msg\n";
  }
}

sub error_log {
  my $msg=shift;
  if ($dest eq "syslog") {
    syslog("err", $msg);
  }
  else {
    print STDERR "Error: $msg\n";
  }
}

sub debug_log {
  my $msg=shift;
  if ($dest eq "syslog") {
    syslog("debug", $msg)
  }
  else {
    print STDERR "Debug: $msg\n";
  }

}

sub warning_log {
  my $msg=shift;
  if ($dest eq "syslog") {
    syslog("warning", $msg);
  }
  else {
    print STDERR "Warning: $msg\n";
  }
}

1;
