# Copyright (C) 2004-2019 Daniel Verite

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
# This plugin pulls large attachments off outgoing mail,
# uploads them to an SSH server and replace them with an URL.
#
# It should be declared in the manitou-mdx configuration file like this:
#
# outgoing_plugins = attach_uploader_ssh({host=>"ssh-server", login=>"your_login", password=>"your_password", maxsize=>1000000, path="/path/to/dir", base_url=>"http://www.example.org[/dir]"})
#
# 'maxsize' is the size in bytes over which an attachment gets uploaded,
#  and defaults to 1Mbytes (1048576)
# 'path' defaults to nothing
########################################################################

package Manitou::Plugins::attach_uploader_ssh;

use strict;
use File::Temp;
use Data::Dumper;

use Net::SFTP::Foreign;
use Digest::MD5 qw(md5_base64);


# Add a note to the outgoing message when attachments are
# converted to URLs in the outgoing message
my $add_notes=1;

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
  foreach my $k ("host", "login", "base_url") {
    if (!defined($self->{args}->{$k})) {
      print STDERR "'$k' entry is missing in init hash parameter\n";
      print STDERR 'Usage: outgoing_plugins = attach_uploader({host=>"ssh-server-name", login=>"ssh username", password=>"optional password", base_url=>"http://www.domain.tld[/directory] [,maxsize=>1000000] [,path="remote upload path"]})', "\n";
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
  my ($self,$obj,$plctxt)=@_;

  my $a=$self->{args};
  my $fname=sanitize_filename($obj->head->recommended_filename);

  my $ftp = $plctxt->{ftp};
  if (!defined $ftp) {
    my %ssh_args=(host=>$a->{host},
		  user=>$a->{login});
    $ssh_args{'password'}=$a->{password} if (defined $a->{password});
    $ftp = Net::SFTP::Foreign->new(%ssh_args);
    $plctxt->{ftp} = $ftp;
  }

  if (defined($a->{path})) {
    if (!$ftp->setcwd($a->{path})) {
      die "Cannot change current directory " . $ftp->error;
    }
  }

  my $dest_dir = $plctxt->{dest_dir};

  # We don't check for an error on mkdir since the directory may exist already
  $ftp->mkdir($dest_dir);

  $ftp->setcwd($dest_dir) or die "Cannot change directory to $dest_dir: " . $ftp->error;

  # TODO: stream into the ftp object
  # my $io=$obj->bodyhandle->open("r");
  # but doesn't work at the moment
  my $tmpfh = File::Temp->new(UNLINK=>0) or die "Cannot create a temporary file\n";
  $obj->bodyhandle->print($tmpfh);
  close($tmpfh);

  if (!$ftp->put($tmpfh->filename, $fname)) {
    warn "Cannot put file $tmpfh->filename to $fname". $ftp->error;
    $fname=undef;
  }
  unlink($tmpfh->filename);

  return defined $fname ? $a->{base_url} . "/$dest_dir/$fname" : undef;
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
    $sthu=$dbh->prepare("UPDATE notes SET note=coalesce(note,'')||? WHERE mail_id=?");
    $sthu->execute("\n$txt", $mail_id);
  }
  $sthu->finish;
}

sub files_size {
  my $top=shift;
  my $sum=0;
  foreach my $p ($top->parts) {
    if (defined($p->head->recommended_filename)) {
      $p->sync_headers({Length=>'COMPUTE'});
      $sum += $p->head->get("Content-Length");
    }
  }
  return $sum;
}

sub process {
  my ($self,$ctxt)=@_;
  my $top=$ctxt->{mimeobj};
  # Upload files only if the total size of named parts is greater than
  # the max size
  if ($top->is_multipart && files_size($top)>=$self->{args}->{maxsize}) {
    my @keep;
    my @urls;

    my %plctxt;
    my $dest_dir = md5_base64("cf-".$ctxt->{mail_id}.rand(1e9));
    $dest_dir =~ tr|+/|-@|;  # replace + and / to create a directory name for an URL
    $plctxt{dest_dir} = $dest_dir;

    foreach my $p ($top->parts) {
      if (defined($p->head->recommended_filename)) {
	my $url=upload($self, $p, \%plctxt);
	if (defined $url) {
	  push @urls, $url;
	  next;		# don't keep that part
	}
      }
      push @keep, $p;		# keep unchanged parts
    }
    $plctxt{ftp}->disconnect() if (defined $plctxt{ftp});

    if (@urls>0) {
      # reconstruct the message
      $top->parts(\@keep);
      foreach (@urls) {
	$top->attach("Data" => "\r\nThe attached file is available at:\r\n$_",
		     "Type"=> "text/plain",
		     "Disposition" => "inline",
		     'Encoding' => '-SUGGEST'
		    );
      }
      if ($add_notes) {
	update_note($ctxt->{dbh}, $ctxt->{mail_id}, \@urls);
      }
    }
  }
  1;
}


1;
