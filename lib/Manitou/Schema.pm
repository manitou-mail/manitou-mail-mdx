# Copyright (C) 2004-2011 Daniel Verite

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

package Manitou::Schema;

use strict;
use vars qw(@ISA @EXPORT_OK);
use Carp;

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(current_version supported_versions upgrade_schema_statements
		create_data_statements create_function_statements
		create_table_statements create_trigger_statements);

sub current_version {
  return "1.2.0";
}

sub supported_versions {
  return ("0.9.12", "1.0.0", "1.0.1", "1.0.2", "1.1.0", "1.2.0");
}

my $create_script=<<EOF;
CREATE TABLE identities (
  identity_id INT primary key default nextval('seq_identity_id'),
  email_addr TEXT NOT NULL,
  username TEXT,
  xface TEXT,
  signature TEXT
);

CREATE TABLE mail (
 mail_id  int,
 sender  text,
 recipients  text,
 sender_fullname text,
 subject text,
 msg_date timestamptz default now(),
 sender_date timestamptz,
 identity_id INT REFERENCES identities(identity_id),
 status  INT,
 mod_user_id INT,
 thread_id INT,
 message_id text,
 in_reply_to INT,
 date_processed timestamptz,
 priority INT default 0,
 flags int default 0,
 raw_size int
);
CREATE UNIQUE INDEX pk_mail_idx ON mail(mail_id);
CREATE INDEX mail_in_replyto_idx ON mail(in_reply_to);
CREATE INDEX mail_message_id_idx ON mail(message_id);
CREATE INDEX mail_date_idx ON mail(msg_date);
CREATE INDEX mail_thread_idx ON mail(thread_id);

CREATE TABLE notes (
  mail_id int REFERENCES mail(mail_id),
  note text,
  last_changed timestamptz default now()
);
CREATE UNIQUE INDEX notes_idx ON notes(mail_id);

CREATE TABLE mail_status (
  mail_id int,
  status int
);
CREATE UNIQUE INDEX pk_mail_status_idx ON mail_status(mail_id);

CREATE TABLE header (
 mail_id  INT REFERENCES mail(mail_id),
 lines  TEXT
);
create unique index pk_header_idx on header(mail_id);

CREATE TABLE body (
 mail_id  INT REFERENCES mail(mail_id),
 bodytext TEXT,
 bodyhtml TEXT
);
create unique index pk_body_idx on body(mail_id);

CREATE TABLE attachments (
 attachment_id INT primary key,
 mail_id  INT REFERENCES mail(mail_id),
 content_type VARCHAR(300),
 content_size INT,
 filename VARCHAR(300),
 charset VARCHAR(30),
 mime_content_id TEXT
);
CREATE INDEX idx_attachments_mail_id ON Attachments(mail_id);

CREATE TABLE attachment_contents (
 attachment_id INT REFERENCES attachments(attachment_id),
 content OID,
 fingerprint TEXT
);
CREATE UNIQUE INDEX attch_ct_idx ON attachment_contents(attachment_id);
CREATE INDEX attach_ct_fp_idx ON attachment_contents(fingerprint);

CREATE TABLE users (
 user_id  INT PRIMARY KEY CHECK (user_id>0),
 fullname VARCHAR(300),
 login  VARCHAR(80),
 email TEXT,
 custom_field1 TEXT,
 custom_field2 TEXT,
 custom_field3 TEXT
);
CREATE UNIQUE INDEX users_login_idx ON users(login);

CREATE TABLE tags (
 tag_id INT,
 name  VARCHAR(300),
 parent_id INT
);
CREATE UNIQUE INDEX tag_id_pk ON tags(tag_id);
ALTER TABLE tags ADD CONSTRAINT parent_tag_fk
 FOREIGN KEY (parent_id) REFERENCES tags(tag_id);

CREATE TABLE mail_tags (
 mail_id INT REFERENCES mail(mail_id),
 tag INT REFERENCES tags(tag_id),
 agent INT,
 date_insert timestamptz default now()
);
CREATE UNIQUE INDEX mail_tags_idx ON mail_tags(mail_id,tag);

CREATE TABLE config (
 conf_key VARCHAR(100) not null,
 value text,
 conf_name VARCHAR(100),
 date_update timestamptz
);

CREATE TABLE files (
 mail_id  INT,
 filename VARCHAR(300)
);

CREATE TABLE addresses (
 addr_id INT PRIMARY KEY,
 email_addr  VARCHAR(300) UNIQUE,
 name  VARCHAR(300),
 nickname varchar(300),
 last_sent_to timestamptz,
 last_recv_from timestamptz,
 notes text,
 invalid int default 0,
 recv_pri int default 0,
 nb_sent_to int,
 nb_recv_from int
);

CREATE TABLE mail_addresses (
 mail_id INT REFERENCES mail(mail_id),
 addr_id INT REFERENCES addresses(addr_id),
 addr_type SMALLINT,
 addr_pos SMALLINT
);
CREATE INDEX mail_addresses_addrid_idx ON mail_addresses(addr_id);
CREATE INDEX mail_addresses_mailid_idx ON mail_addresses(mail_id);

CREATE TABLE programs (
 program_name varchar(256),
 content_type varchar(256),
 conf_name varchar(100)
);

CREATE TABLE mime_types (
 suffix varchar(20) NOT NULL,
 mime_type varchar(100) NOT NULL
);

CREATE TABLE runtime_info (
  rt_key varchar(100) not null,
  rt_value text
);
CREATE UNIQUE INDEX runtime_info_pk ON runtime_info(rt_key);


CREATE TABLE words (
 word_id int PRIMARY KEY,
 wordtext varchar(50)
);
CREATE UNIQUE INDEX wordtext_idx ON words(wordtext);

CREATE TABLE non_indexable_words (
  wordtext varchar(50)
);

CREATE TABLE filter_expr (
  expr_id int PRIMARY KEY,
  name varchar(100),
  user_lastmod int,
  last_update timestamptz default now(),
  expression text,
  direction char(1) default 'I',
  apply_order real UNIQUE,
  last_hit timestamptz
);
CREATE UNIQUE INDEX expr_idx ON filter_expr(name);

CREATE TABLE filter_action (
 expr_id int REFERENCES filter_expr(expr_id),
 action_order smallint,
 action_arg text,
 action_type varchar(100)
);
CREATE UNIQUE INDEX filter_action_idx ON filter_action(expr_id,action_order);

CREATE TABLE filter_log (
 expr_id int,   -- No reference to filter_expr, we don't want any constraint here
 mail_id int,   -- No reference to mail to be able to delete mail without touching this table
 hit_date timestamptz default now()
);

CREATE TABLE user_queries (
 title text NOT NULL,
 sql_stmt text
);
CREATE UNIQUE INDEX user_queries_idx ON user_queries(title);

CREATE TABLE tags_words (
  tag_id int REFERENCES tags(tag_id),
  word_id int REFERENCES words(word_id),
  counter int
) WITHOUT OIDS;
CREATE INDEX tags_words_idx ON tags_words(word_id);

CREATE TABLE forward_addresses (
 to_email_addr  varchar(300),
 forward_to  text
);
CREATE UNIQUE INDEX fwa_idx ON forward_addresses(to_email_addr);

CREATE TABLE raw_mail (
  mail_id int REFERENCES mail(mail_id),
  mail_text oid
);
CREATE UNIQUE INDEX idx_raw_mail ON raw_mail(mail_id);

CREATE TABLE inverted_word_index (
  word_id int REFERENCES words(word_id),
  part_no int,
  mailvec bytea,
  nz_offset int default 0
);
CREATE UNIQUE INDEX iwi_idx ON inverted_word_index(word_id,part_no);

CREATE TABLE jobs_queue (
 job_id serial,
 mail_id int,
 job_type varchar(100),
 job_args text,
 status smallint
);
CREATE UNIQUE INDEX jobs_pk_idx ON jobs_queue(job_id);

CREATE TABLE global_notepad (
 contents text,
 last_modified timestamptz
);
EOF

my %tables=("mailing_definition"=> <<'EOT'
CREATE TABLE mailing_definition (
  mailing_id serial PRIMARY KEY,
  title text,
  sender_email text,
  creation_date timestamptz default now(),
  end_date timestamptz,
  text_template text,
  html_template text,
  header_template text,
  csv_columns text
)
EOT
,"mailing_run" => <<'EOT'
CREATE TABLE mailing_run (
  mailing_id int REFERENCES mailing_definition(mailing_id),
  status smallint,
  throughput float,
  nb_total int,
  nb_sent int,
  last_sent timestamptz
)
EOT
,"mailing_data" => <<'EOT'
CREATE TABLE mailing_data (
  mailing_data_id serial primary key,
  mailing_id int REFERENCES mailing_definition(mailing_id),
  recipient_email text,
  csv_data text,
  sent character
)
EOT
,"mail_template" => <<'EOT'
CREATE TABLE mail_template (
  template_id serial PRIMARY KEY,
  title text,
  body_text text,
  body_html text,
  header text,
  creation_date timestamptz default now()
)
EOT
,"import_mbox" => <<'EOT'
CREATE TABLE import_mbox (
  import_id serial PRIMARY KEY,
  tag_id int,
  mail_status smallint,
  apply_filters character,
  completion real,
  status smallint,
  filename text,
  auto_purge character
)
EOT
,"import_message" => <<'EOT'
CREATE TABLE import_message (
  import_id integer,
  mail_number integer,
  encoded_mail bytea,
  status smallint,
  mail_id int
)
EOT
);

my %object_comments=(
"mailing_run.status" => "0=not started, 1=running, 2=stopped, 3=finished",
"mail_addresses.addr_type" => "1=from, 2=to, 3=cc, 4=reply-to, 5=bcc",
"import_message.status" => "0=new, 1=imported, 2=cancelled",
"import_mbox.status" => "0=not started, 1=running, 2=aborted, 3=finished",
"import_mbox.auto_purge" => "Delete the row in this table when the import has successfully completed",
);

my %functions=("insert_mail" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION insert_mail() RETURNS TRIGGER AS $$
BEGIN
	IF NEW.status&(256+32+16)=0 THEN
	  -- The message is not yet sent, archived, or trashed
	  INSERT INTO mail_status(mail_id,status) VALUES(new.mail_id,new.status);
	END IF;
	RETURN new;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,

"update_mail" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION update_mail() RETURNS TRIGGER AS $$
DECLARE
 rc int4;
BEGIN
   IF new.status!=old.status THEN
	IF NEW.status&(256+32+16)=0 THEN
	  -- The message is not yet sent, archived, or trashed
	  UPDATE mail_status
	    SET status = new.status
	   WHERE mail_id = new.mail_id;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  if rc=0 THEN
	    INSERT INTO mail_status(mail_id,status) VALUES(new.mail_id,new.status);
	  END IF;
	ELSE
	  -- The mail has been "processed"
	  DELETE FROM mail_status
	   WHERE mail_id = new.mail_id;
	END IF;
   END IF;
   RETURN new;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,

"delete_mail" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION delete_mail() RETURNS TRIGGER AS $$
BEGIN
	DELETE FROM mail_status WHERE mail_id=OLD.mail_id;
	RETURN old;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,

"delete_msg" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION delete_msg(integer) RETURNS integer AS $$
DECLARE
	id ALIAS FOR $1;
	attch RECORD;
	cnt integer;
	o oid;
BEGIN
  DELETE FROM notes WHERE mail_id=id;
  DELETE FROM mail_addresses WHERE mail_id=id;
  DELETE FROM header WHERE mail_id=id;
  DELETE FROM body WHERE mail_id=id;
  DELETE FROM mail_tags WHERE mail_id=id;

  FOR attch IN SELECT a.attachment_id,c.content,c.fingerprint FROM attachments a, attachment_contents c WHERE a.mail_id=id AND c.attachment_id=a.attachment_id LOOP
    cnt=0;
    IF attch.fingerprint IS NOT NULL THEN
      -- check if that content is shared with another message's attachment
      SELECT count(*) INTO cnt FROM attachment_contents WHERE fingerprint=attch.fingerprint AND attachment_id!=attch.attachment_id;
    END IF;
    IF (cnt=0) THEN
      PERFORM lo_unlink(attch.content);
    END IF;
    DELETE FROM attachment_contents WHERE attachment_id=attch.attachment_id;
  END LOOP;

  DELETE FROM attachments WHERE mail_id=id;
  UPDATE mail SET in_reply_to=NULL WHERE in_reply_to=id;

  SELECT mail_text INTO o FROM raw_mail WHERE mail_id=id;
  IF FOUND THEN
     PERFORM lo_unlink(o);
     DELETE FROM raw_mail WHERE mail_id=id;
  END IF;

  DELETE FROM mail WHERE mail_id=id;
  IF (FOUND) THEN
	  RETURN 1;
  ELSE
	  RETURN 0;
  END IF;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,

"delete_msg_set" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION delete_msg_set(in_array_mail_id int[]) RETURNS int AS $$
DECLARE
 cnt int;
BEGIN
 cnt:=0;
 FOR idx IN array_lower(in_array_mail_id,1)..array_upper(in_array_mail_id,1) LOOP
   cnt:=cnt + delete_msg(in_array_mail_id[idx]);
 END LOOP;
 RETURN cnt;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,

"replace_header_field" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION replace_header_field(in_mail_id int, field text, val text) RETURNS void
AS $$
declare
 cnt int;
BEGIN
  IF val IS NULL THEN
    -- Remove the entries
    UPDATE header SET lines = replace(regexp_replace(lines, '^'||field||':.*?$', E'\n', 'ngi'), E'\n\n', '')
     WHERE mail_id=in_mail_id
      AND lines ~* ('(?n)^'||field||':.*$');
    -- Special case for the first line
    UPDATE header SET lines = regexp_replace(lines, '^'||field||E':.*?\n', '', 'i')      WHERE mail_id=in_mail_id AND lines ~* ('^' ||field||E':.*?\n');
  ELSE
    -- First try to replace existing header entries by the new value
    UPDATE header SET lines = regexp_replace(lines, '^'||field||E':.*$', field||': '||val, 'gni')
     WHERE mail_id=in_mail_id
      AND lines ~* ('(?n)^'||field||':.*$');

    GET DIAGNOSTICS cnt=ROW_COUNT;
    -- If the update didn't find any entry, append the field to the header
    IF cnt=0 THEN
      UPDATE header SET lines = lines||field||': '||val||chr(10)
        WHERE mail_id=in_mail_id;
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
,

"trash_msg" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION trash_msg(in_mail_id integer, in_op integer) RETURNS integer AS $$
DECLARE
new_status int;
BEGIN
  UPDATE mail SET status=status|16,mod_user_id=in_op WHERE mail_id=in_mail_id;
  SELECT INTO new_status status FROM mail WHERE mail_id=in_mail_id;
  RETURN new_status;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,
"trash_msg_set" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION trash_msg_set(in_array_mail_id int[], in_op int) RETURNS int AS $$
DECLARE
cnt int;
BEGIN
  UPDATE mail SET status=status|16, mod_user_id=in_op WHERE mail_id=any(in_array_mail_id);
  GET DIAGNOSTICS cnt=ROW_COUNT;
  return cnt;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,
"untrash_msg" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION untrash_msg(in_mail_id int, in_op int) RETURNS int AS $$
DECLARE
new_status int;
BEGIN
  UPDATE mail SET status=status&(~16),mod_user_id=in_op WHERE mail_id=in_mail_id;
  SELECT INTO new_status status FROM mail WHERE mail_id=in_mail_id;
  RETURN new_status;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,
"update_note_flag" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION update_note_flag() RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    UPDATE mail SET flags=flags&(~2) WHERE mail_id=OLD.mail_id;
    RETURN OLD;
  ELSIF (TG_OP = 'INSERT') THEN
    UPDATE mail SET flags=flags|2 WHERE mail_id=NEW.mail_id;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE 'plpgsql'
EOFUNCTION
,
"wordsearch_get_parts" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION wordsearch_get_parts(in_words text[]) RETURNS SETOF integer
AS $$
DECLARE
 var_nb_words integer := array_upper(in_words,1);
BEGIN
  RETURN QUERY select part_no FROM inverted_word_index WHERE word_id in (select word_id from words where wordtext=any(in_words))  GROUP BY part_no HAVING count(*)=var_nb_words;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
,
"wordsearch" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION wordsearch(in_words text[]) RETURNS SETOF integer
AS $$
DECLARE
 var_nb_words integer := array_upper(in_words,1);
 var_part_no integer;
 cnt integer;
 b1 integer;
 b2 integer;
 len integer;
 i integer;
 j integer;
 var_vect bytea;
 and_vect bytea; -- vectors ANDed together
 var_nz_offset integer;
BEGIN
  FOR var_part_no IN (select part_no FROM inverted_word_index WHERE word_id in (select word_id from words where wordtext=any(in_words))  GROUP BY part_no HAVING count(*)=var_nb_words ORDER BY part_no)
  LOOP
    cnt:=0;
    FOR var_vect,var_nz_offset IN SELECT mailvec,nz_offset FROM inverted_word_index WHERE word_id in (select word_id from words where wordtext=any(in_words)) AND part_no=var_part_no LOOP
      IF (var_nz_offset>0) THEN
	var_vect:=repeat(E'\\000', var_nz_offset)::bytea || var_vect;
      END IF;
      IF (cnt=0) THEN
	and_vect:=var_vect;  -- first vector
      ELSE
	-- next vectors
	-- reduce result if necessary
	IF (length(and_vect) > length(var_vect)) THEN
	  and_vect:=substring(and_vect for length(var_vect));
	END IF;
        len:=length(and_vect)-1;
	FOR i in 0..len LOOP
	  b1:=get_byte(and_vect, i);
	  b2:=get_byte(var_vect, i);
          IF (b1&b2 <> b1) THEN
	    SELECT set_byte(and_vect, i, b1&b2) INTO and_vect;
	  END IF;
	END LOOP;
      END IF;
      cnt:=cnt+1;
    END LOOP; -- on vectors

    -- extract the set of mail_id's for this part_no from the vector
    len:=length(and_vect)-1;
    IF (len>=0) THEN  -- len might be NULL OR -1 if no result at all
      FOR i IN 0..len LOOP
	b1:=get_byte(and_vect,i);
	FOR j IN 0..7 LOOP
	  IF ((b1&(1<<j))!=0) THEN
	    RETURN NEXT var_part_no*16384+(i*8)+j+1; -- hit
	  END IF;
	  j:=j+1;
	END LOOP;
      END LOOP;
    END IF;
  END LOOP; -- on part_no
  RETURN;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
,
"wordsearch_part" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION wordsearch_part(in_words text[], in_part_no integer) RETURNS SETOF integer
AS $$
DECLARE
 var_nb_words integer := array_upper(in_words,1);
 cnt integer;
 b1 integer;
 b2 integer;
 len integer;
 i integer;
 j integer;
 var_vect bytea;
 and_vect bytea; -- vectors ANDed together
 var_nz_offset integer;
BEGIN
    cnt:=0;
    FOR var_vect,var_nz_offset IN SELECT mailvec,nz_offset FROM inverted_word_index WHERE word_id in (select word_id from words where wordtext=any(in_words)) AND part_no=in_part_no LOOP
      IF (var_nz_offset>0) THEN
	var_vect:=repeat(E'\\000', var_nz_offset)::bytea || var_vect;
      END IF;
      IF (cnt=0) THEN
	and_vect:=var_vect;  -- first vector
      ELSE
	-- next vectors
	-- reduce result if necessary
	IF (length(and_vect) > length(var_vect)) THEN
	  and_vect:=substring(and_vect for length(var_vect));
	END IF;
        len:=length(and_vect)-1;
	FOR i in 0..len LOOP
	  b1:=get_byte(and_vect, i);
	  b2:=get_byte(var_vect, i);
          IF (b1&b2 <> b1) THEN
	    SELECT set_byte(and_vect, i, b1&b2) INTO and_vect;
	  END IF;
	END LOOP;
      END IF;
      cnt:=cnt+1;
    END LOOP; -- on vectors

    -- extract the set of mail_id's for this part_no from the vector
    len:=length(and_vect)-1;
    IF (len>=0) THEN  -- len might be NULL OR -1 if no result at all
      FOR i IN 0..len LOOP
	b1:=get_byte(and_vect,i);
	FOR j IN 0..7 LOOP
	  IF ((b1&(1<<j))!=0) THEN
	    RETURN NEXT in_part_no*16384+(i*8)+j+1; -- hit
	  END IF;
	  j:=j+1;
	END LOOP;
      END LOOP;
    END IF;

  RETURN;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
);

my %triggers=(
 "update_mail" => q{CREATE TRIGGER update_mail AFTER UPDATE ON mail
 FOR EACH ROW EXECUTE PROCEDURE update_mail()},

 "insert_mail" => q{CREATE TRIGGER insert_mail AFTER INSERT ON mail
 FOR EACH ROW EXECUTE PROCEDURE insert_mail()},

 "delete_mail" => q{CREATE TRIGGER delete_mail AFTER DELETE ON mail
 FOR EACH ROW EXECUTE PROCEDURE delete_mail()},

 "update_note" => q{CREATE TRIGGER update_note AFTER INSERT OR DELETE ON notes
 FOR EACH ROW EXECUTE PROCEDURE update_note_flag()}

);

sub extract_statements {
  my @statements;;
  foreach (split( /\s*;\s*/m, $create_script)) {
    if ($_ ne "") {
      push @statements, $_;
    }
  }
  return @statements;
}

sub create_table_statements {
  my @stmt=extract_statements($create_script);
  for my $t qw(mailing_definition mailing_run mailing_data mail_template import_mbox import_message) {
    push @stmt, $tables{$t};
  }
  foreach my $c (keys %object_comments) {
    push @stmt, sql_comment($c);
  }

  return @stmt;

}

sub create_function_statements {
  return (values %functions);
}

sub create_trigger_statements {
  return (values %triggers);
}

sub create_sequence_statements {
  return map { "CREATE SEQUENCE $_" } ("seq_tag_id","seq_mail_id", "seq_thread_id", "seq_addr_id", "seq_attachment_id", "seq_identity_id", "seq_filter_expr_id", "seq_word_id");
}

sub create_data_statements {
  my %types=
    ('txt' => 'text/plain',
     'htm' => 'text/html',
     'html' => 'text/html',
     'xml' => 'text/xml',
     'rtf' => 'application/rtf',
     'zip' => 'application/zip',
     'doc' => 'application/msword',
     'xls' => 'application/vnd.ms-excel',
     'pdf' => 'application/pdf',
     'tar' => 'application/x-tar',
     'jpg' => 'image/jpeg',
     'gif' => 'image/gif',
     'png' => 'image/png',
     'bmp' => 'image/bmp'
    );

  my @statements;
  while (my ($k,$v) = each %types) {
    push @statements, "INSERT INTO mime_types VALUES('$k', '$v')";
  }
  return @statements;
}

sub sql_comment {
  my $col=shift;
  die "Non-existing SQL comment for column $col" if (!exists $object_comments{$col});
  # TODO: see how we could use $dbh->quote() to protect the comment
  # right now we don't have $dbh
  return "COMMENT ON COLUMN $col IS '" . $object_comments{$col} . "'";
}

sub function_exists {
  my ($dbh,$fn)=@_;
  my $sth=$dbh->prepare("SELECT 1 FROM information_schema.routines WHERE routine_schema='public' AND routine_name=?");
  $sth->execute($fn) or die $sth->errstr;
  my @r=$sth->fetchrow_array;
  return @r>0 && $r[0]==1;
}

sub table_constraint_exists {
  my ($dbh,$tbl,$constraint)=@_;
  my $sth=$dbh->prepare("SELECT 1 FROM information_schema.table_constraints WHERE table_name=? AND constraint_name=? AND table_schema='public'");
  $sth->execute($tbl, $constraint) or die $sth->errstr;
  my @r=$sth->fetchrow_array;
  return @r>0 && $r[0]==1;
}

sub upgrade_schema_statements {
  my ($dbh,$from,$to)=@_;
  my @stmt;
  if ($from eq "0.9.12" && $to eq "1.0.0") {
    push @stmt,
      ("ALTER TABLE body DROP COLUMN textsize",
       "ALTER TABLE body ADD bodyhtml TEXT",
       "ALTER TABLE header DROP COLUMN header_size",
       "ALTER TABLE mail DROP COLUMN msg_day",
       "ALTER TABLE mail DROP COLUMN operator",
       "ALTER TABLE mail RENAME attachments TO flags",
       "ALTER TABLE mail RENAME COLUMN mod_userid TO mod_user_id",
       "UPDATE mail SET flags=1 WHERE flags!=0",
       "UPDATE mail SET flags=flags|2 WHERE mail_id in (select mail_id from notes)"
      );

    push @stmt, "CREATE TABLE global_notepad (contents text, last_modified timestamptz)";
    push @stmt, $functions{"update_note_flag"};
    push @stmt, $triggers{"update_note"};
    push @stmt, $functions{"trash_msg"};
    push @stmt, $functions{"trash_msg_set"};
    push @stmt, $functions{"untrash_msg"};
  }
  elsif ($from eq "1.0.0" && $to eq "1.0.1") {
    push @stmt, ("ALTER TABLE users ADD email TEXT",
		 "ALTER TABLE users ADD custom_field1 TEXT",
		 "ALTER TABLE users ADD custom_field2 TEXT",
		 "ALTER TABLE users ADD custom_field3 TEXT",
		 "ALTER TABLE users ADD CONSTRAINT user_id_gt0 CHECK(user_id>0)"
		);
  }
  elsif ($from eq "1.0.1" && $to eq "1.0.2") {
  }
  elsif ($from eq "1.0.2" && $to eq "1.1.0") {
    push @stmt, "ALTER TABLE jobs_queue ALTER COLUMN job_type TYPE varchar(100)";
    push @stmt, $tables{"mailing_definition"};
    push @stmt, $tables{"mailing_run"};
    push @stmt, $tables{"mailing_data"};
    push @stmt, $tables{"mail_template"};
    push @stmt, sql_comment("mailing_run.status");

  }
  elsif ($from eq "1.1.0" && $to eq "1.2.0") {
    push @stmt, $functions{"wordsearch"} if (!function_exists($dbh, "wordsearch"));
    push @stmt, $functions{"wordsearch_get_parts"}  if (!function_exists($dbh, "wordsearch_get_parts"));
    push @stmt, $functions{"wordsearch_part"}  if (!function_exists($dbh, "wordsearch_part"));

    push @stmt, ( # Filters
		 "ALTER TABLE filter_expr ADD apply_order real UNIQUE",
		 "ALTER TABLE filter_expr ADD last_hit timestamptz",
		 "UPDATE filter_expr SET apply_order=expr_id",
		 "CREATE SEQUENCE seq_filter_expr_id",
		 "SELECT setval('seq_filter_expr_id', 1+coalesce(x,0)) from (select max(expr_id) as x from filter_expr) m",
		 "UPDATE filter_action SET action_type='discard',action_arg='trash' WHERE action_type='status' AND action_arg='T'",
		 # Merge of mailboxes into identities
		 "CREATE SEQUENCE seq_identity_id",
		 "ALTER TABLE identities ADD identity_id INT",
		 "INSERT INTO identities (email_addr,identity_id) SELECT name,mbox_id FROM mailboxes WHERE NOT EXISTS (select 1 from identities where email_addr=mailboxes.name)",
		 "UPDATE identities i SET identity_id=(SELECT mbox_id FROM mailboxes m WHERE m.name=i.email_addr)",
		 "SELECT setval('seq_identity_id',1+coalesce(x,0)) from (select max(identity_id) as x from identities) m",
		 "UPDATE identities SET identity_id=nextval('seq_identity_id') WHERE identity_id is null",
		 "ALTER TABLE identities ALTER COLUMN identity_id SET DEFAULT nextval('seq_identity_id')",
		 "ALTER TABLE identities ALTER COLUMN email_addr TYPE text",
		 "ALTER TABLE identities ALTER COLUMN username TYPE text", 
		 "ALTER TABLE mail RENAME COLUMN mbox_id TO identity_id",
		 "DROP TABLE mailboxes CASCADE",
		 "ALTER TABLE identities ADD PRIMARY KEY (identity_id)",
		 "ALTER TABLE mail ADD CONSTRAINT mail_identity_id_fkey FOREIGN KEY(identity_id) REFERENCES identities(identity_id)",
		 # New columns
		 "ALTER TABLE mail ADD raw_size INT",
		 "ALTER TABLE mail RENAME toname TO recipients",
		 "ALTER TABLE mail ALTER COLUMN recipients TYPE text",
		 "UPDATE mail m set recipients=(select (regexp_matches(lines, E'\\nTo: (.*?)\\n'))[1] from header where mail_id=m.mail_id)",
		 "INSERT INTO config(conf_key,value) VALUES('display/auto_sender_column','1')"
		);
    push @stmt, sql_comment("mail_addresses.addr_type");
    push @stmt, $functions{"replace_header_field"};
  }

  return @stmt;
}

1;
