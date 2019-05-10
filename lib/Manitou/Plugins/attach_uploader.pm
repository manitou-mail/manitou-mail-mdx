# Copyright (C) 2005-2019 Daniel Verite

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
# attach_uploader plugin
# This plugin pulls large attachments off outgoing mail,
# uploads them to an FTP server and replace them with an URL.
#
# It should be declared in the manitou-mdx configuration file like this:
#
# outgoing_plugins = attach_uploader({host=>"ftp-server-name", login=>"your_login", password=>"your_password", maxsize=>1000000, path="ftp_path_to_cd_into", base_url=>"http://www.example.org[/whatever]"})
#
# 'maxsize' is the size in bytes over which an attachment gets uploaded,
#  and defaults to 1Mbytes (1000000)
# 'path' defaults to nothing
########################################################################

package Manitou::Plugins::attach_uploader;

use strict;
use File::Temp;

require Net::FTP;

# Add a note to the outgoing message when attachments are
# converted to URLs in the outgoing message
my $add_notes=1;

# Arguments: login,passwd[,maxsize in bytes]
sub init {
  my $dbh=shift;
  my $self=bless {};
  if (@_<1) {
    print STDERR "attach_uploader plugin: missing arguments\n";
    return undef;
  }
  $self->{args}=$_[0];
  if (!defined($self->{args}->{maxsize})) {
    $self->{args}->{maxsize}=1024*1024;
  }
  foreach my $k ("host", "login", "password", "base_url") {
    if (!defined($self->{args}->{$k})) {
      print STDERR "'$k' entry is missing in init hash parameter\n";
      print STDERR 'Usage: outgoing_plugins = attach_uploader({host=>"ftp-server-name", login=>"your_login", password=>"your_password", base_url=>"http://www.domain.tld[/whatever] [,maxsize=>1000000] [,path="ftp_path_to_cd_into"]})', "\n";
      return undef;
    }
  }
  return $self;
}

sub finish {
  1;
}

# remove undesirable characters
sub sanitize_filename {
  my $f=$_[0];
  $f =~ s/\/\\\"\'\<\>//g;
  $f =~ s/\s/_/g;
  $f =~ tr/\x00-\x1F//d;
  return $f;
}

sub upload {
  my ($self,$obj)=@_;

  my $a=$self->{args};
  my $fname=sanitize_filename($obj->head->recommended_filename);

  my $ftp = Net::FTP->new($a->{host}, Debug => 0)
    or die "Cannot connect to $a->{host}: $@";

  if (!$ftp->login($a->{login}, $a->{password})) {
    print STDERR "Cannot login ", $ftp->message, "\n";
    return undef;
  }

  if (defined($a->{path})) {
    if (!$ftp->cwd($a->{path})) {
      print STDERR "Cannot change current directory ", $ftp->message, "\n";
      return undef;
    }
  }

  # TODO: stream into the ftp object
  # my $io=$obj->bodyhandle->open("r");
  # but doesn't work at the moment
  my $tmpfh = File::Temp->new(UNLINK=>0) or die "Cannot create a temporary file\n";
  $obj->bodyhandle->print($tmpfh);
  close($tmpfh);

  $ftp->binary;
  if (!$ftp->put($tmpfh->filename, $fname)) {
    print STDERR "Cannot put file ", $ftp->message, "\n";
    $fname=undef;
  }
  $ftp->quit;
  unlink($tmpfh->filename);

  return defined($fname) ?$a->{base_url} . "/$fname" : undef;
}

sub update_note {
  my ($dbh,$mail_id,$u)=@_;
  my $sth=$dbh->prepare("SELECT note FROM notes WHERE mail_id=?");
  $sth->execute($mail_id);
  my $txt;
  foreach (@{$u}) {
    $txt.="<$_>\n";
  }
  my $sthu;
  if ($sth->rows==0) {
    $sthu=$dbh->prepare("INSERT INTO notes(mail_id,note) VALUES(?,?)");
    $sthu->execute($mail_id, $txt);
  }
  else {
    $sthu=$dbh->prepare("UPDATE notes SET note=note||? WHERE mail_id=?");
    $sthu->execute("\n$txt", $mail_id);
  }
  $sthu->finish;
}

sub process {
  my ($self,$ctxt)=@_;
  my $top=$ctxt->{mimeobj};
  if ($top->is_multipart) {
    my @keep;
    my @urls;
    foreach my $p ($top->parts) {
      if (defined($p->head->recommended_filename)) {
	$p->sync_headers({Length=>'COMPUTE'});
	if ($p->head->get("Content-Length")>=$self->{args}->{maxsize}) {
	  my $url=upload($self, $p);
	  if (defined($url)) {
	    push @urls, $url;
	    next;		# don't keep that part
	  }
	}
      }
      push @keep, $p;		# keep unchanged parts
    }
    if (@urls>0) {
      # reconstruct the message
      $top->parts(\@keep);
      foreach (@urls) {
	$top->attach(Data => "<$_>\n");
      }
      if ($add_notes) {
	update_note($ctxt->{dbh}, $ctxt->{mail_id}, \@urls);
      }
    }
  }
  1;
}

1;
