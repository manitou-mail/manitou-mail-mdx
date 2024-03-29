# Copyright (C) 2004-2024 Daniel Verite

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
		create_table_statements create_trigger_statements
		partition_words);

sub current_version {
  return "1.7.3";
}

sub supported_versions {
  return ("0.9.12", "1.0.0", "1.0.1", "1.0.2", "1.1.0", "1.2.0", "1.3.0",
	  "1.3.1", "1.4.0", "1.5.0", "1.6.0", "1.7.0", "1.7.1", "1.7.2",
          "1.7.3");
}

my $create_script=<<EOF;
CREATE TABLE identities (
  identity_id INT primary key default nextval('seq_identity_id'),
  email_addr TEXT NOT NULL UNIQUE,
  username TEXT,
  xface TEXT,
  signature TEXT,
  root_tag INT, -- references tags(tag_id)
  restricted BOOL default false
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
 email_addr text UNIQUE,
 name  text,
 nickname text,
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

my @post_create_table_statements = (
  # foreign keys that can't be declared at create time because
  # of cycles in dependencies

  "ALTER TABLE identities ADD CONSTRAINT identities_root_tag_fkey FOREIGN KEY (root_tag) REFERENCES tags(tag_id)"

);

my %tables=(
"mailing_definition"=> <<'EOT'
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
,"identities_permissions" => <<'EOT'
CREATE TABLE identities_permissions (
  role_oid oid,
  identity_id int REFERENCES identities(identity_id)
);
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
,"tags_counters" => <<'EOT'
CREATE TABLE tags_counters (
  tag_id integer not null references tags(tag_id) on delete cascade,
  cnt integer,
  temp boolean not null
)
EOT
,"thread_action" => <<'EOT'
CREATE TABLE thread_action (
  thread_id integer,
  mail_id integer,
  action_type integer CHECK (action_type IN (1,2,3)),
  date_insert timestamptz default now()
  -- one and only one of (mail_id,thread_id) must be null
  CHECK ((mail_id IS NULL) <> (thread_id IS NULL))
)
EOT
);

my %indexes = (
 "thread_action_idx1" =>
    "CREATE INDEX thread_action_idx1 ON thread_action(mail_id) WHERE mail_id is not null",
 "thread_action_idx2" =>
    "CREATE INDEX thread_action_idx2 ON thread_action(thread_id) WHERE thread_id is not null"
);

my @ordered_tables = qw(
  mailing_definition mailing_run mailing_data
  mail_template
  import_mbox import_message
  identities_permissions
  tags_counters
  thread_action
);



my %object_comments=(
"mailing_run.status" => "0=not started, 1=running, 2=stopped, 3=finished",
"mail_addresses.addr_type" => "1=from, 2=to, 3=cc, 4=reply-to, 5=bcc",
"import_message.status" => "0=new, 1=imported, 2=cancelled",
"import_mbox.status" => "0=not started, 1=running, 2=aborted, 3=finished",
"import_mbox.auto_purge" => "Delete the row in this table when the import has successfully completed",
);

my %functions=(
"add_mail_tags" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION add_mail_tags(in_tag_id INT, in_mail_IDs INT[], OUT ignored int[])
AS $$
BEGIN
  ignored := in_mail_IDs;

 WITH v(mid) AS (
  -- insert not-yet existing (tag,mail) tuples and return the set of inserted mail_id
  INSERT INTO mail_tags(mail_id, tag)
    SELECT t.mid, in_tag_id FROM unnest(in_mail_IDs) AS t(mid)
    WHERE NOT EXISTS
     (SELECT 1 FROM mail_tags WHERE tag=in_tag_id AND mail_id=t.mid)
    RETURNING mail_id
  ),
  -- return the set of id_excl
  id_excl(id) AS ( select t.id FROM unnest(in_mail_IDs) AS t(id) EXCEPT select mid FROM v),
  -- insert the count deltas for the non-trashed messages
  ins AS (INSERT INTO tags_counters(tag_id,cnt,temp)
   SELECT in_tag_id, count(*), true FROM (select mail_id FROM v JOIN mail ON (mail_id=v.mid)
     WHERE mail.status&(32+16)=32) AS s HAVING count(*)<>0)
  --
  SELECT array(SELECT id FROM id_excl)
  INTO ignored;
END
$$ LANGUAGE plpgsql
EOFUNCTION
,
"archive_msg_set" => <<'EOFUNCTION'
CREATE or REPLACE FUNCTION archive_msg_set(in_mail_id int[], in_user_id int)
 RETURNS TABLE(tag_id int, cnt int) as $$
BEGIN
 RETURN QUERY
 WITH v(mid) AS (
   UPDATE mail m SET status = status|32, mod_user_id = in_user_id
     FROM unnest(in_mail_id) AS list(mid)
    WHERE status&32 = 0
     AND m.mail_id = list.mid
    RETURNING list.mid
 )
 -- partial counters
 INSERT INTO tags_counters(tag_id,cnt,temp)
   SELECT tag, count(*), true FROM mail_tags JOIN v ON (mail_id=mid)
     GROUP BY tag
 RETURNING tags_counters.tag_id, tags_counters.cnt;
END;
$$ language plpgsql
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
"remove_mail_tags" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION remove_mail_tags(in_tag_id INT,
   in_mail_IDs INT[], OUT ignored int[])
AS $$
BEGIN
  WITH v AS (
    DELETE FROM mail_tags WHERE tag = in_tag_id AND mail_ID = ANY(in_mail_IDs)
      RETURNING mail_id
  ),
  -- mail archived and non-trashed are subtracted from tags counters
  ins AS (INSERT INTO tags_counters(tag_id,cnt,temp)
   SELECT in_tag_id, -1*count(*), true FROM v JOIN mail USING(mail_id)
      WHERE mail.status&(32+16)=32 HAVING count(*)<>0)
  SELECT array(SELECT * FROM unnest(in_mail_IDs) EXCEPT SELECT v.mail_id FROM v)
    INTO ignored;
END
$$ LANGUAGE plpgsql
EOFUNCTION
,

"status_mask" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION status_mask(text) returns int as $$
  select case $1
  when 'archived' then 32
  when 'read' then 1
  when 'replied' then 4
  when 'sent' then 256
  else null
  end
$$ language sql immutable
EOFUNCTION
,

"trash_msg" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION trash_msg(in_mail_id integer, in_op integer) RETURNS integer AS $$
DECLARE
new_status int;
BEGIN
  UPDATE mail SET status=status|16,mod_user_id=in_op WHERE mail_id=in_mail_id
    RETURNING status INTO new_status;
  RETURN new_status;
END;
$$ LANGUAGE plpgsql
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
$$ LANGUAGE plpgsql
EOFUNCTION
,
"trash_msg_set_tags" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION trash_msg_set_tags(in_mail_id int[], in_user_id int)
  RETURNS TABLE(tag_id int, cnt int) AS $$
DECLARE
cnt int;
BEGIN
 RETURN QUERY
 WITH v(mid) AS (
   UPDATE mail m SET status = status|16, mod_user_id = in_user_id
     FROM unnest(in_mail_id) AS list(mid)
    WHERE status&16 = 0
     AND m.mail_id = list.mid
    RETURNING m.mail_id,m.status
 )
 -- tags of messages put in trashcan are subtracted from
 -- cached counts when they were archived previously
 INSERT INTO tags_counters(tag_id,cnt,temp)
   SELECT tag, -1*count(*), true FROM mail_tags JOIN v ON (mail_id=mid)
     WHERE v.status&32=32
     GROUP BY tag
 RETURNING tags_counters.tag_id, tags_counters.cnt;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
,

"object_permissions" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION object_permissions(ability text)
 RETURNS table(objname text, objtype text, privtype text)
 language plpgsql
AS $$
DECLARE
 -- the tables that need to be SELECTable for a read-only user
 tbl_ro text[]:='{
      "addresses", "attachment_contents", "attachments",
      "body", "config", "header", "identities", "identities_permissions",
      "mail", "mail_addresses", "mail_tags", "programs",
      "notes", "runtime_info", "tags", "users", "user_queries" }';

 -- the tables that need to be UPDATE'able for a read-write user
 tbl_upd text[]:='{
      "addresses", "attachment_contents", "attachments",
      "body", "config", "header", "mail",
      "mail_addresses", "mail_tags",
      "notes" }';

 -- the tables that need to be DELETE'able for a deleter
 tbl_del text[]:='{
      "attachment_contents", "attachments",
      "body", "header", "mail",
      "mail_addresses", "mail_tags",
      "notes", "raw_mail"   }';

 -- the sequences that require USAGE permission to add messages
 seq_insert text[]:='{
   seq_addr_id,
   seq_mail_id,
   seq_tag_id,
   seq_thread_id,
   seq_attachment_id,
   jobs_queue_job_id_seq }';


BEGIN
  IF ability = 'read' THEN
    -- permissions for "reader" ability
    FOR objname, objtype, privtype IN
     (SELECT a.objname, 'table', 'select'
       FROM unnest(tbl_ro) as a(objname))
    LOOP
       RETURN NEXT;
    END LOOP;

    objname := 'wordsearch(text[],text[])';
    objtype := 'function';
    privtype := 'execute';
    RETURN NEXT;

  ELSIF ability = 'update' THEN
    FOR objname, objtype, privtype IN
     (SELECT a.objname, 'table', 'update'
       FROM unnest(tbl_upd) as a(objname))
    LOOP
       RETURN NEXT;
    END LOOP;

  ELSIF ability = 'delete' THEN
    objname := 'delete_msg(int)';
    objtype := 'function';
    privtype := 'execute';
    RETURN NEXT;

    objname := 'delete_msg_set(int[])';
    objtype := 'function';
    privtype := 'execute';
    RETURN NEXT;

    RETURN QUERY SELECT a.objname, 'table'::text, 'delete'::text
       FROM unnest(tbl_del) as a(objname);

  ELSIF ability = 'trash' THEN
    RETURN QUERY SELECT n::text,t::text,pt::text FROM (VALUES
       ('execute', 'function', 'trash_msg(int,int)'),
       ('execute', 'function', 'trash_msg_set(int[],int)'),
       ('update', 'table', 'mail')
      ) AS tbl(pt,t,n);

  ELSIF ability = 'compose' THEN
    FOR objname, objtype, privtype IN
     (SELECT a.objname, 'table', 'insert'
       FROM unnest(tbl_upd) as a(objname))
    LOOP
       RETURN NEXT;
    END LOOP;
    FOR objname, objtype, privtype IN
     (SELECT a.objname, 'sequence', 'usage'
       FROM unnest(seq_insert) as a(objname))
    LOOP
       RETURN NEXT;
    END LOOP;
    RETURN NEXT;

  ELSIF ability = 'admin-level1' THEN
    -- ability to create/modify/delete filters and tags
    RETURN QUERY
     SELECT a.objname, 'table'::text, b.privtype
       FROM (values('tags'),('filter_expr'),('filter_action')) as a(objname)
       	    CROSS JOIN (values('insert'),('update'),('delete')) as b(privtype)
    ;
  END IF;

END
$$
EOFUNCTION
,

"set_identity_permissions" => <<'EOFUNCTION'
CREATE OR REPLACE FUNCTION set_identity_permissions(
       in_oid oid, --oid of role
       in_identities int[], -- references to identities.identity_id
       in_perms character[] -- for future use (permission types). Pass array['A'] here.
) RETURNS VOID AS $$
DECLARE
  stmt text;
BEGIN
  IF in_oid IS NULL THEN RETURN; END IF;
  DELETE FROM identities_permissions WHERE role_oid = in_oid;
  INSERT INTO identities_permissions(role_oid,identity_id)
    SELECT in_oid, * FROM unnest(in_identities);
  BEGIN
    EXECUTE 'DROP POLICY ident_' || in_oid || ' ON mail';
  EXCEPTION WHEN undefined_object THEN
    --do nothing
  END;
    IF (array_length(in_identities,1) >= 1) THEN
      SELECT 'CREATE POLICY ident_' || in_oid || ' ON MAIL FOR ALL TO ' ||
        quote_ident(rolname) || ' USING (identity_id IN (' ||
	array_to_string(in_identities,',') || '))'
	FROM pg_roles WHERE oid = in_oid
      INTO stmt;
      raise debug 'policy stmt = %', stmt;
      IF stmt IS NOT NULL THEN
        EXECUTE stmt;
      END IF;
    END IF;
END $$ language plpgsql
EOFUNCTION
,
"transition_status_tags" => << 'EOFUNCTION'
CREATE OR REPLACE FUNCTION transition_status_tags(in_mail_id integer, new_status integer)
 RETURNS TABLE(cnt_tag_id integer, diff integer) AS
$$
DECLARE
  ostatus int;
BEGIN
  SELECT status INTO ostatus FROM mail WHERE mail_id = in_mail_id;
  IF new_status=-1 THEN  -- mail is to be deleted
    RETURN QUERY
      INSERT INTO tags_counters(tag_id, cnt, temp)
	SELECT tag,-1,true FROM mail_tags
          WHERE mail_id = in_mail_id AND ostatus&(32+16)=32 -- archived and !trashed
	RETURNING tag_id,-1;
    RETURN;
  END IF;

  -- mail is going to trashcan
  --
    -- if (!archived)=>archived
    IF (ostatus&32=0 AND new_status&32=32) THEN
      IF (new_status&16=0) THEN
        RETURN QUERY
	  INSERT INTO tags_counters(tag_id, cnt, temp)
	    SELECT tag,1,true FROM mail_tags WHERE mail_id = in_mail_id
	    RETURNING tag_id,1;
      END IF;
    -- if archived=>(!archived and !trashed)
    ELSIF (ostatus&32=32) AND (new_status&(32+16)=0) THEN
      IF (ostatus&16=0) THEN
        RETURN QUERY
	  INSERT INTO tags_counters(tag_id, cnt, temp)
	    SELECT tag,-1,true FROM mail_tags WHERE mail_id = in_mail_id
	    RETURNING tag_id,-1;
      END IF;
    -- if (archived and !trashed)=>(archived and trashed)
    ELSIF (ostatus&(32+16)=32) AND (new_status&(32+16)=32+16) THEN
        RETURN QUERY
	  INSERT INTO tags_counters(tag_id, cnt, temp)
	    SELECT tag,-1,true FROM mail_tags WHERE mail_id = in_mail_id
	    RETURNING tag_id,-1;
    -- if (archived and trashed)=>(archived and !trashed)
    ELSIF (ostatus&(32+16)=32+16) AND (new_status&(32+16)=32) THEN
        RETURN QUERY
	  INSERT INTO tags_counters(tag_id, cnt, temp)
	    SELECT tag,1,true FROM mail_tags WHERE mail_id = in_mail_id
	    RETURNING tag_id, 1;
    END IF;
END;
$$ language plpgsql
EOFUNCTION
,
"update_tags_counters" => << 'EOFUNCTION'
CREATE OR REPLACE FUNCTION update_tags_counters() RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    -- delete all tag counters (permanent and temp) for this tag
    DELETE FROM tags_counters WHERE tag_id=OLD.tag_id;
  ELSIF (TG_OP = 'INSERT') THEN
    -- add new permanent tag counter for this tag
    INSERT INTO tags_counters(tag_id,temp,cnt) VALUES(NEW.tag_id,false,0);
  END IF;
  RETURN NULL;
END $$ LANGUAGE plpgsql
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
CREATE OR REPLACE FUNCTION wordsearch(in_words text[],out_words text[]) RETURNS SETOF integer
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
 var2_vect bytea;
 and_vect bytea:=null; -- vectors ANDed together
 excl_vect bytea; -- for words that must not be in the text (out_words)
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
        IF array_upper(out_words,1)>0 THEN
	  excl_vect:=null;
	  FOR var2_vect IN SELECT repeat(E'\\000', nz_offset)::bytea||mailvec FROM inverted_word_index WHERE word_id in (select word_id from words where wordtext=any(out_words)) AND part_no=var_part_no
	  LOOP
	    IF excl_vect is null THEN
	      excl_vect:=var2_vect;
	    ELSE
	      IF (length(excl_vect) > length(var2_vect)) THEN
		var2_vect:=var2_vect||repeat(E'\\000', length(excl_vect)-length(var2_vect));
	      ELSEIF (length(excl_vect) < length(var2_vect)) THEN
		excl_vect:= excl_vect || repeat(E'\\000', length(var2_vect)-length(excl_vect));
	      END IF;
	      -- excl_vect := excl_vect OR var2_vect
	      len:=length(excl_vect)-1;
	      FOR i in 0..len LOOP
		b1:=get_byte(excl_vect, i);
		b2:=get_byte(var2_vect, i);
		IF (b1|b2 <> b1) THEN
		  SELECT set_byte(excl_vect, i, b1|b2) INTO excl_vect;
		END IF;
	      END LOOP;
	    END IF;
	  END LOOP; -- for each word to exclude
          IF excl_vect is not null THEN
	    -- invert excl_vect (bitwise NOT) to make it an AND mask against
	    -- the vectors of the words included in the search
	      len:=length(excl_vect)-1;
	      FOR i in 0..len LOOP
		b1:=get_byte(excl_vect, i);
		SELECT set_byte(excl_vect, i, ~b1) INTO excl_vect;
	      END LOOP;
	  END IF;
        END IF; -- out_words is not empty
	and_vect:=excl_vect;
      END IF;
      IF and_vect IS NOT NULL THEN
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
      ELSE
	and_vect:=var_vect;
      END IF;
      cnt:=cnt+1;
    END LOOP; -- on vectors of the same part_no

    -- extract the set of mail_id's for this part_no from the vector
    len:=length(and_vect)-1;
    IF (len>=0) THEN  -- len might be NULL OR -1 if no result at all
      FOR i IN 0..len LOOP
	b1:=get_byte(and_vect,i);
	FOR j IN 0..7 LOOP
	  IF ((b1&(1<<j))!=0) THEN
	    RETURN NEXT var_part_no*16384+(i*8)+j+1; -- hit
	  END IF;
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
	END LOOP;
      END LOOP;
    END IF;

  RETURN;
END;
$$ LANGUAGE plpgsql
EOFUNCTION
);

my %triggers=(
 "update_note" => q{CREATE TRIGGER update_note AFTER INSERT OR DELETE ON notes
 FOR EACH ROW EXECUTE PROCEDURE update_note_flag()} ,
 "update_tags" => q{CREATE TRIGGER tag_trigger AFTER INSERT OR DELETE ON tags
 FOR EACH ROW EXECUTE PROCEDURE update_tags_counters()}
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
  foreach my $t (@ordered_tables) {
    push @stmt, $tables{$t};
  }
  foreach my $c (keys %object_comments) {
    push @stmt, sql_comment($c);
  }
  foreach (@post_create_table_statements) {
    push @stmt, $_;
  }
  foreach my $i (keys %indexes) {
    push @stmt, $indexes{$i};
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
     'bmp' => 'image/bmp',
     'eml' => 'message/rfc822'
    );

  my @v;
  while (my ($k,$v) = each %types) {
    push @v, "('$k', '$v')";
  }
  my @v;
  while (my ($k,$v) = each %types) {
    push @v, "('$k', '$v')";
  }
  return ("INSERT INTO mime_types VALUES " . join(",", @v));
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
  elsif ($from eq "1.2.0" && $to eq "1.3.0") {
    push @stmt, "ALTER TABLE jobs_queue ADD status SMALLINT";
    push @stmt, $tables{"import_mbox"};
    push @stmt, $tables{"import_message"};
    push @stmt, $functions{"wordsearch"};
  }
  elsif ($from eq "1.3.0" && $to eq "1.4.0") {
    # no change in schema
  }
  elsif ($from eq "1.4.0" && $to eq "1.5.0") {
    # no change in schema
  }
  elsif ($from eq "1.5.0" && $to eq "1.6.0") {
    push @stmt, $functions{"object_permissions"};
    push @stmt, ("ALTER TABLE addresses ALTER COLUMN name TYPE text",
		 "ALTER TABLE addresses ALTER COLUMN email_addr TYPE text",
		 "ALTER TABLE addresses ALTER COLUMN nickname TYPE text");
    push @stmt, ("ALTER TABLE identities ADD root_tag integer REFERENCES tags(tag_id)",
		 "ALTER TABLE identities ADD restricted bool DEFAULT false");
    # Update mail.message_id in case it was still varchar(N)
    push @stmt, "ALTER TABLE mail ALTER COLUMN message_id TYPE text";
    push @stmt, $tables{"identities_permissions"};
    push @stmt, $functions{"set_identity_permissions"};
  }
  elsif ($from eq "1.6.0" && $to eq "1.7.0") {
    push @stmt, "ALTER TABLE identities ADD CONSTRAINT identities_email_addr_key UNIQUE(email_addr)";
    push @stmt, "CREATE INDEX current_mail_idx ON mail(status) WHERE status&32=0";
    push @stmt, "DROP TRIGGER insert_mail ON mail";
    push @stmt, "DROP TRIGGER update_mail ON mail";
    push @stmt, "DROP TRIGGER delete_mail ON mail";
    push @stmt, $functions{"object_permissions"};  # updated to drop references to mail_status
    push @stmt, $functions{"status_mask"};
    push @stmt, "DROP TABLE mail_status";

    # materialized tags counts
    push @stmt, $tables{"tags_counters"};
    push @stmt, $functions{"add_mail_tags"};
    push @stmt, $functions{"remove_mail_tags"};
    push @stmt, $functions{"archive_msg_set"};
    push @stmt, $functions{"trash_msg_set_tags"};
    push @stmt, $functions{"update_tags_counters"};
    push @stmt, $functions{"transition_status_tags"};
    push @stmt, q{INSERT INTO tags_counters(tag_id,cnt,temp)
      SELECT tag_id,coalesce(cnt,0),false
        FROM tags LEFT JOIN (select tag,count(*) AS cnt
          FROM mail_tags JOIN mail USING(mail_id)
            WHERE mail.status&(16+32)=32 GROUP BY tag) AS t
       ON (tag=tag_id)};
    push @stmt, $triggers{"update_tags"};
  }
  elsif ($from eq "1.7.0" && $to eq "1.7.1") {
    push @stmt, $functions{"object_permissions"};
    push @stmt, $tables{"thread_action"};
    push @stmt, $indexes{"thread_action_idx1"};
    push @stmt, $indexes{"thread_action_idx2"};
  }
  elsif ($from eq "1.7.1" && $to eq "1.7.2") {
    push @stmt, q{ALTER TABLE tags_counters DROP CONSTRAINT tags_counters_tag_id_fkey,
		  ADD CONSTRAINT tags_counters_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE};
  }

  return @stmt;
}

sub upsert_runtime_info {
  my ($dbh, $name, $value) = @_;
  # $dbh->do("LOCK TABLE runtime_info IN EXCLUSIVE MODE");
  my $r=$dbh->do("UPDATE runtime_info SET rt_value=? WHERE rt_key=?", undef, $value, $name);
  if ($r eq "0E0") { # zero row affected
    $dbh->do("INSERT INTO runtime_info VALUES(rt_key,rt_value) VALUES(?,?)", undef, $name, $value);
  }
}

sub do_or_print {
  my ($dry_run, $dbh, $query)=@_;
  if ($dry_run) {
    print "$query;\n";
    return "0E0";
  }
  else {
    return $dbh->do($query);
  }
}

sub partition_words {
  my ($dbh, $fti_conf, $on, $dry_run) = @_;
  if ($on && $fti_conf->{words_partitioning}) {
    die "Words partitioning is already in effect according to runtime_info.";
  }
  if (!$on && !$fti_conf->{words_partitioning}) {
    die "Words partitioning is not in effect according to runtime_info.";
  }
  if ($on) {
    my @parts;
    $dbh->begin_work;
    foreach my $c ('a'..'z') {
      my $q1 = "create table words_$c as select * from words where substr(wordtext,1,1)='$c'";
      do_or_print($dry_run, $dbh, $q1);
      push @parts, "words_$c";
      my $q2 = "create unique index words_${c}_idx on words_$c(wordtext)";
      do_or_print($dry_run, $dbh, $q2);
    }

    do_or_print($dry_run, $dbh, "CREATE TABLE words_09 AS select * from words where ascii(substr(wordtext,1,1)) between ascii('0') and ascii('9')");
      push @parts, "words_09";

    do_or_print($dry_run, $dbh, "CREATE TABLE words2 AS select * from words where not (ascii(substr(wordtext,1,1)) between ascii('a') and ascii('z')) and not (ascii(substr(wordtext,1,1)) between ascii('0') and ascii('9'))");
    push @parts, "words2";

    my @select_parts = map { " SELECT * FROM $_" } @parts;
    my $create_view = "CREATE VIEW words AS \n" . join(" UNION ALL\n", @select_parts);
    do_or_print($dry_run, $dbh, "DROP TABLE words");
    do_or_print($dry_run, $dbh, $create_view);

    upsert_runtime_info($dbh, "words_partitioning", "1") unless($dry_run);
    $dbh->commit;
  }
  else {
    $dbh->begin_work;
    my $table_name = "words_tmp_" . int(rand(100000));
    do_or_print($dry_run, $dbh, "CREATE TABLE $table_name AS select * from words");
    do_or_print($dry_run, $dbh, "DROP VIEW words");
    do_or_print($dry_run, $dbh, "ALTER TABLE $table_name RENAME TO words");
    foreach my $c ('a'..'z') {
      do_or_print($dry_run, $dbh, "DROP TABLE words_$c");
    }
    do_or_print($dry_run, $dbh, "DROP TABLE words_09");
    do_or_print($dry_run, $dbh, "DROP TABLE words2");

    upsert_runtime_info($dbh, "words_partitioning", "0") unless($dry_run);
    $dbh->commit;
  }
}

1;
