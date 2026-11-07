use crate::model::*;
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;
use std::{path::Path, sync::Arc};

type Job = Box<dyn FnOnce(&mut Connection) + Send>;
#[derive(Clone)]
pub struct Store {
    tx: Arc<std::sync::mpsc::Sender<Job>>,
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let mut conn = Connection::open(path)?;
        let tables: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        anyhow::ensure!(
            (tables == 0 && version == 0) || version == 5,
            "Unsupported uaoi database. Use a fresh state directory; old state cannot be loaded."
        );
        conn.pragma_update(None, "user_version", 5)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, agent TEXT NOT NULL, native TEXT NOT NULL, UNIQUE(agent,native));
          CREATE TABLE IF NOT EXISTS bindings(platform TEXT, space TEXT, thread TEXT, session TEXT UNIQUE REFERENCES sessions(id), PRIMARY KEY(platform,space,thread));
          CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, state TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
          CREATE TABLE IF NOT EXISTS submissions(id TEXT PRIMARY KEY, session TEXT, text TEXT, state TEXT);
          CREATE TABLE IF NOT EXISTS events(session TEXT, key TEXT, body TEXT, PRIMARY KEY(session,key));
          CREATE TABLE IF NOT EXISTS deliveries(id INTEGER PRIMARY KEY, agent TEXT NOT NULL, platform TEXT, space TEXT, thread TEXT, key TEXT, text TEXT, message TEXT, buttons TEXT, links TEXT NOT NULL, state TEXT DEFAULT 'pending', UNIQUE(platform,space,thread,key));
          CREATE TABLE IF NOT EXISTS session_titles(session TEXT PRIMARY KEY REFERENCES sessions(id),title TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS conversation_titles(platform TEXT,space TEXT,thread TEXT,desired TEXT NOT NULL,applied TEXT,next_attempt INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(platform,space,thread));
          CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, session TEXT, body TEXT, resolved INTEGER DEFAULT 0);
          CREATE TABLE IF NOT EXISTS binding_state(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL);
          INSERT OR IGNORE INTO binding_state VALUES(1,0);
          CREATE TABLE IF NOT EXISTS actions(id TEXT PRIMARY KEY, target TEXT NOT NULL, body TEXT NOT NULL);
")?;
        conn.execute("DELETE FROM actions", [])?;
        // Requests belong to a live CLI connection and never survive a service restart.
        conn.execute("UPDATE requests SET resolved=1", [])?;
        // An interrupted send without a platform receipt has an unknown outcome.
        conn.execute("UPDATE deliveries SET state=CASE WHEN message IS NULL THEN 'unknown' ELSE 'pending' END WHERE state='sending'", [])?;
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("uaoi-sqlite".into())
            .spawn(move || {
                for job in rx {
                    job(&mut conn);
                }
            })?;
        Ok(Self { tx: Arc::new(tx) })
    }
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Box::new(move |db| {
                let _ = tx.send(f(db));
            }))
            .map_err(|_| anyhow::anyhow!("Database worker stopped"))?;
        rx.await.context("Database worker stopped")?
    }
    pub async fn ensure_session(&self, agent: &str, native: &str) -> Result<Session> {
        let (agent, native) = (agent.to_owned(), native.to_owned());
        self.call(move |db| {
            db.execute(
                "INSERT OR IGNORE INTO sessions VALUES(?1,?2,?3)",
                params![uuid::Uuid::new_v4().to_string(), agent, native],
            )?;
            Ok(db.query_row(
                "SELECT id,agent,native FROM sessions WHERE agent=?1 AND native=?2",
                params![agent, native],
                row_session,
            )?)
        })
        .await
    }
    pub async fn session(&self, id: &str) -> Result<Session> {
        let id = id.to_owned();
        self.call(move |db| {
            Ok(db.query_row(
                "SELECT id,agent,native FROM sessions WHERE id=?1",
                [id],
                row_session,
            )?)
        })
        .await
    }
    pub async fn bind(&self, target: &Conversation, session: &str) -> Result<()> {
        let (t, s) = (target.clone(), session.to_owned());
        self.call(move |db| {
            let old: Option<String> = db
                .query_row(
                    "SELECT session FROM bindings WHERE platform=?1 AND space=?2 AND thread=?3",
                    params![t.platform, t.space, t.thread],
                    |r| r.get(0),
                )
                .optional()?;
            anyhow::ensure!(
                old.as_ref().is_none_or(|old| old == &s),
                "Topic already bound to another session"
            );
            let tx = db.transaction()?;
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO bindings VALUES(?1,?2,?3,?4)",
                params![t.platform, t.space, t.thread, s],
            )?;
            let session = tx.query_row(
                "SELECT id,agent,native FROM sessions WHERE id=?1",
                [&s],
                row_session,
            )?;
            let name: Option<String> = tx
                .query_row(
                    "SELECT title FROM session_titles WHERE session=?1",
                    [&s],
                    |r| r.get(0),
                )
                .optional()?;
            queue_title(&tx, &t, &display_title(&session, name.as_deref()))?;
            if inserted != 0 {
                tx.execute(
                    "UPDATE binding_state SET revision=revision+1 WHERE id=1",
                    [],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
    pub async fn bound(&self, target: &Conversation) -> Result<Option<Session>> {
        let t = target.clone();
        self.call(move|db|Ok(db.query_row("SELECT s.id,s.agent,s.native FROM sessions s JOIN bindings b ON b.session=s.id WHERE platform=?1 AND space=?2 AND thread=?3",params![t.platform,t.space,t.thread],row_session).optional()?)).await
    }
    pub async fn targets(&self, session: &str) -> Result<Vec<Conversation>> {
        let s = session.to_owned();
        self.call(move |db| {
            Ok(db
                .prepare("SELECT platform,space,thread FROM bindings WHERE session=?1")?
                .query_map([s], |r| {
                    Ok(Conversation {
                        platform: r.get(0)?,
                        space: r.get(1)?,
                        thread: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
    }
    pub async fn bindings(&self) -> Result<(u64, Vec<BindingRecord>)> {
        self.call(|db| {
            let revision = db.query_row("SELECT revision FROM binding_state WHERE id=1", [], |r| r.get(0))?;
            let records = db.prepare("SELECT s.id,s.agent,s.native,b.platform,b.space,b.thread FROM bindings b JOIN sessions s ON s.id=b.session ORDER BY s.agent,s.native")?
                .query_map([], |r| Ok(BindingRecord { session:row_session(r)?, target:Conversation { platform:r.get(3)?,space:r.get(4)?,thread:r.get(5)? } }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((revision,records))
        }).await
    }
    pub async fn replace_bindings(&self, revision: u64, records: Vec<BindingRecord>) -> Result<()> {
        self.call(move |db| {
            let tx = db.transaction()?;
            let current: u64 = tx.query_row("SELECT revision FROM binding_state WHERE id=1", [], |r| r.get(0))?;
            anyhow::ensure!(current == revision, "Bindings changed while editing; reopen the editor and retry");
            let old = tx.prepare("SELECT s.id,s.agent,s.native,b.platform,b.space,b.thread FROM bindings b JOIN sessions s ON s.id=b.session")?
                .query_map([], |r| Ok(BindingRecord { session:row_session(r)?,target:Conversation { platform:r.get(3)?,space:r.get(4)?,thread:r.get(5)? } }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for record in old {
                if records.iter().any(|next| next.session.id == record.session.id && next.target == record.target) { continue; }
                let t=record.target;
                tx.execute("UPDATE deliveries SET state='cancelled' WHERE platform=?1 AND space=?2 AND thread=?3 AND state IN ('pending','sending')", params![t.platform,t.space,t.thread])?;
                tx.execute("DELETE FROM actions WHERE target=?1", [serde_json::to_string(&t)?])?;
                tx.execute("DELETE FROM conversation_titles WHERE platform=?1 AND space=?2 AND thread=?3", params![t.platform,t.space,t.thread])?;
            }
            tx.execute("DELETE FROM bindings", [])?;
            for record in records {
                let t=&record.target;
                tx.execute("INSERT INTO bindings VALUES(?1,?2,?3,?4)", params![t.platform,t.space,t.thread,record.session.id])?;
                let title: Option<String> = tx.query_row("SELECT title FROM session_titles WHERE session=?1", [&record.session.id], |r| r.get(0)).optional()?;
                queue_title(&tx,t,&display_title(&record.session,title.as_deref()))?;
            }
            tx.execute("UPDATE binding_state SET revision=revision+1 WHERE id=1", [])?;
            tx.commit()?;
            Ok(())
        }).await
    }
    pub async fn native_session(&self, agent: &str, native: &str) -> Result<Option<Session>> {
        let (agent, native) = (agent.to_owned(), native.to_owned());
        self.call(move |db| {
            Ok(db
                .query_row(
                    "SELECT id,agent,native FROM sessions WHERE agent=?1 AND native=?2",
                    params![agent, native],
                    row_session,
                )
                .optional()?)
        })
        .await
    }
    pub async fn title(&self, session: &Session) -> Result<String> {
        let session = session.clone();
        self.call(move |db| {
            let name: Option<String> = db
                .query_row(
                    "SELECT title FROM session_titles WHERE session=?1",
                    [&session.id],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(display_title(&session, name.as_deref()))
        })
        .await
    }
    pub async fn set_title(&self, session: &Session, name: Option<&str>) -> Result<()> {
        let (session, name) = (session.clone(), name.unwrap_or_default().trim().to_owned());
        self.call(move |db| {
            let tx=db.transaction()?;
            tx.execute("INSERT INTO session_titles VALUES(?1,?2) ON CONFLICT(session) DO UPDATE SET title=excluded.title",params![session.id,name])?;
            let title=display_title(&session,Some(&name));
            let targets=tx.prepare("SELECT platform,space,thread FROM bindings WHERE session=?1")?.query_map([&session.id],|r|Ok(Conversation{platform:r.get(0)?,space:r.get(1)?,thread:r.get(2)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for target in targets {queue_title(&tx,&target,&title)?;}
            tx.commit()?;Ok(())
        }).await
    }
    pub async fn pending_titles(&self) -> Result<Vec<TitleChange>> {
        self.call(|db|Ok(db.prepare("SELECT c.platform,c.space,c.thread,c.desired,s.agent FROM conversation_titles c JOIN bindings b USING(platform,space,thread) JOIN sessions s ON s.id=b.session WHERE (c.applied IS NULL OR c.applied<>c.desired) AND c.next_attempt<=unixepoch() ORDER BY c.rowid LIMIT 25")?.query_map([],|r|Ok(TitleChange{agent:r.get(4)?,target:Conversation{platform:r.get(0)?,space:r.get(1)?,thread:r.get(2)?},title:r.get(3)?}))?.collect::<rusqlite::Result<Vec<_>>>()?)).await
    }
    pub async fn title_is_current(&self, change: &TitleChange) -> Result<bool> {
        let c = change.clone();
        self.call(move |db|Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM conversation_titles WHERE platform=?1 AND space=?2 AND thread=?3 AND desired=?4 AND (applied IS NULL OR applied<>desired) AND next_attempt<=unixepoch() AND EXISTS(SELECT 1 FROM bindings b JOIN sessions s ON s.id=b.session WHERE b.platform=?1 AND b.space=?2 AND b.thread=?3 AND s.agent=?5))",params![c.target.platform,c.target.space,c.target.thread,c.title,c.agent],|r|r.get(0))?)).await
    }
    pub async fn title_applied(&self, change: &TitleChange) -> Result<()> {
        let c = change.clone();
        self.call(move |db| {
            // Record what actually reached the platform, even if a newer name arrived in flight.
            db.execute("UPDATE conversation_titles SET applied=?4 WHERE platform=?1 AND space=?2 AND thread=?3 AND EXISTS(SELECT 1 FROM bindings b JOIN sessions s ON s.id=b.session WHERE b.platform=?1 AND b.space=?2 AND b.thread=?3 AND s.agent=?5)",params![c.target.platform,c.target.space,c.target.thread,c.title,c.agent])?;Ok(())
        }).await
    }
    pub async fn retry_title(
        &self,
        change: &TitleChange,
        delay: std::time::Duration,
    ) -> Result<()> {
        let c = change.clone();
        let delay = delay.as_secs().max(1).min(i64::MAX as u64) as i64;
        self.call(move |db| {
            db.execute("UPDATE conversation_titles SET next_attempt=unixepoch()+?5 WHERE platform=?1 AND space=?2 AND thread=?3 AND desired=?4",params![c.target.platform,c.target.space,c.target.thread,c.title,delay])?;Ok(())
        }).await
    }
    pub async fn claim(&self, id: &str) -> Result<bool> {
        let id = id.to_owned();
        self.call(move |db| {
            Ok(db.execute(
                "INSERT OR IGNORE INTO operations(id,state) VALUES(?1,'unknown')",
                [id],
            )? == 1)
        })
        .await
    }
    pub async fn claim_message(&self, id: &str, detail: &str) -> Result<bool> {
        let (id, detail) = (id.to_owned(), detail.to_owned());
        self.call(move |db| {
            Ok(db.execute(
                "INSERT OR IGNORE INTO operations(id,state,detail) VALUES(?1,'unknown',?2)",
                params![id, detail],
            )? == 1)
        })
        .await
    }
    pub async fn recovery(&self) -> Result<serde_json::Value> {
        self.call(|db| {
            let rows = db
                .prepare("SELECT id,detail FROM operations WHERE state='unknown' ORDER BY rowid")?
                .query_map([], |r| {
                    Ok(json!({"id":r.get::<_,String>(0)?,"detail":r.get::<_,String>(1)?}))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(json!(rows))
        })
        .await
    }
    pub async fn operation(&self, id: &str, state: &str, detail: &str) -> Result<()> {
        let (i, s, d) = (id.to_owned(), state.to_owned(), detail.to_owned());
        self.call(move |db| {
            db.execute(
                "UPDATE operations SET state=?2,detail=?3 WHERE id=?1",
                params![i, s, d],
            )?;
            Ok(())
        })
        .await
    }
    pub async fn operation_record(&self, id: &str) -> Result<Option<(String, String)>> {
        let id = id.to_owned();
        self.call(move |db| {
            Ok(db
                .query_row(
                    "SELECT state,detail FROM operations WHERE id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?)
        })
        .await
    }
    pub async fn origin(&self, id: &str) -> Result<Option<Conversation>> {
        let id = id.to_owned();
        self.call(move |db| {
            let detail: Option<String> = db
                .query_row("SELECT detail FROM operations WHERE id=?1", [id], |r| {
                    r.get(0)
                })
                .optional()?;
            Ok(detail.and_then(|s| serde_json::from_str(&s).ok()))
        })
        .await
    }
    pub async fn submission(&self, id: &str, session: &str, text: &str, state: &str) -> Result<()> {
        let (id, session, text, state) = (
            id.to_owned(),
            session.to_owned(),
            text.to_owned(),
            state.to_owned(),
        );
        self.call(move |db| {
            db.execute("INSERT INTO submissions VALUES(?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET state=CASE WHEN submissions.state='consumed' THEN 'consumed' ELSE excluded.state END",params![id,session,text,state])?;
            Ok(())
        }).await
    }
    pub async fn confirm_input(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move |db| {
            db.execute("UPDATE submissions SET state='consumed' WHERE id=?1", [id])?;
            Ok(())
        })
        .await
    }
    pub async fn unconsumed(&self, session: &str) -> Result<serde_json::Value> {
        let session = session.to_owned();
        self.call(move|db| {
            let rows=db.prepare("SELECT id,text,state FROM submissions WHERE session=?1 AND state IN ('unknown','submitted') ORDER BY rowid")?.query_map([session],|r|Ok(json!({"id":r.get::<_,String>(0)?,"text":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(json!(rows))
        }).await
    }
    pub async fn event(&self, event: &AgentEvent) -> Result<()> {
        let e = event.clone();
        self.call(move|db| {
            db.execute("INSERT INTO events VALUES(?1,?2,?3) ON CONFLICT(session,key) DO UPDATE SET body=excluded.body",params![e.session,e.key,serde_json::to_string(&e)?])?;
            if let Some(r)=&e.request { db.execute("INSERT OR IGNORE INTO requests(id,session,body) VALUES(?1,?2,?3)",params![r.id,e.session,serde_json::to_string(r)?])?; }
            Ok(())
        }).await
    }
    pub async fn request(&self, id: &str) -> Result<(String, PendingRequest)> {
        let id = id.to_owned();
        self.call(move |db| {
            let (s, b): (String, String) = db.query_row(
                "SELECT session,body FROM requests WHERE id=?1 AND resolved=0",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok((s, serde_json::from_str(&b)?))
        })
        .await
    }
    pub async fn resolve(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move |db| {
            db.execute("UPDATE requests SET resolved=1 WHERE id=?1", [id])?;
            Ok(())
        })
        .await
    }
    pub async fn expire_requests(&self, session: &str) -> Result<()> {
        let session = session.to_owned();
        self.call(move |db| {
            db.execute("UPDATE requests SET resolved=1 WHERE session=?1", [&session])?;
            db.execute("UPDATE deliveries SET buttons='[]',text=text || '\nExpired',state=CASE WHEN state='sent' THEN 'pending' ELSE state END WHERE key LIKE ?1 AND buttons<>'[]'", [format!("{session}:request:%")])?;
            Ok(())
        }).await
    }
    pub async fn history(
        &self,
        session: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<AgentEvent>> {
        let s = session.to_owned();
        self.call(move|db|{
            let mut v=db.prepare("SELECT body FROM events WHERE session=?1 ORDER BY rowid DESC LIMIT ?2 OFFSET ?3")?.query_map(params![s,limit,offset],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            v.reverse();v.into_iter().map(|b|Ok(serde_json::from_str(&b)?)).collect()
        }).await
    }
    pub async fn enqueue(
        &self,
        agent: &str,
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<ButtonRow>,
        links: Vec<TextLink>,
    ) -> Result<()> {
        let (agent, t, k, txt) = (
            agent.to_owned(),
            target.clone(),
            key.to_owned(),
            text.to_owned(),
        );
        self.call(move |db| {
            db.execute("INSERT INTO deliveries(platform,space,thread,key,text,buttons,links,agent) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(platform,space,thread,key) DO UPDATE SET agent=excluded.agent,text=excluded.text,buttons=excluded.buttons,links=excluded.links,state=CASE WHEN deliveries.state='sent' AND (deliveries.text<>excluded.text OR deliveries.buttons<>excluded.buttons OR deliveries.links<>excluded.links) THEN 'pending' ELSE deliveries.state END", params![t.platform,t.space,t.thread,k,txt,serde_json::to_string(&buttons)?,serde_json::to_string(&links)?,agent])?;
            Ok(())
        }).await
    }
    pub async fn pending(&self) -> Result<Vec<Delivery>> {
        self.call(|db| {
            let mut stmt=db.prepare("SELECT id,platform,space,thread,text,message,buttons,key,links,agent FROM deliveries WHERE state='pending' ORDER BY id LIMIT 25")?;
            let rows=stmt.query_map([], |r| Ok((r.get::<_,i64>(0)?,Conversation{platform:r.get(1)?,space:r.get(2)?,thread:r.get(3)?},r.get::<_,String>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,String>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?,r.get::<_,String>(9)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|(id,target,text,message,buttons,key,links,agent)| Ok(Delivery{agent,id,key,target,text,message,buttons:serde_json::from_str(&buttons)?,links:serde_json::from_str(&links)?})).collect()
        }).await
    }
    pub async fn delivery_current(&self, d: &Delivery) -> Result<bool> {
        let id = d.id;
        self.call(move |db| {
            Ok(db.query_row(
                "SELECT state IN ('pending','sending') FROM deliveries WHERE id=?1",
                [id],
                |r| r.get(0),
            )?)
        })
        .await
    }
    pub async fn delivery(&self, d: &Delivery, state: &str, message: Option<String>) -> Result<()> {
        let (id, state, text, buttons, links) = (
            d.id,
            state.to_owned(),
            d.text.clone(),
            serde_json::to_string(&d.buttons)?,
            serde_json::to_string(&d.links)?,
        );
        self.call(move |db| {
            db.execute("UPDATE deliveries SET state=CASE WHEN state='cancelled' THEN state WHEN ?2='sent' AND (text<>?4 OR buttons<>?5 OR links<>?6) THEN 'pending' ELSE ?2 END,message=COALESCE(?3,message) WHERE id=?1",params![id,state,message,text,buttons,links])?;
            Ok(())
        }).await
    }
    pub async fn action(&self, target: &Conversation, body: serde_json::Value) -> Result<String> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let (key, target) = (id.clone(), serde_json::to_string(target)?);
        self.call(move |db| {
            db.execute(
                "INSERT INTO actions VALUES(?1,?2,?3)",
                params![key, target, body.to_string()],
            )?;
            Ok(())
        })
        .await?;
        Ok(id)
    }
    pub async fn read_action(&self, id: &str, target: &Conversation) -> Result<serde_json::Value> {
        let (id, target) = (id.to_owned(), serde_json::to_string(target)?);
        self.call(move |db| {
            let body: String = db
                .query_row(
                    "SELECT body FROM actions WHERE id=?1 AND target=?2",
                    params![id, target],
                    |r| r.get(0),
                )
                .context("This button expired; refresh the panel")?;
            Ok(serde_json::from_str(&body)?)
        })
        .await
    }
    pub async fn reply_key(&self, target: &Conversation, message: &str) -> Result<Option<String>> {
        let (t, message) = (target.clone(), message.to_owned());
        self.call(move |db| Ok(db.query_row("SELECT key FROM deliveries WHERE platform=?1 AND space=?2 AND thread=?3 AND message=?4",params![t.platform,t.space,t.thread,message],|r|r.get(0)).optional()?)).await
    }
}
fn row_session(r: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        id: r.get(0)?,
        agent: r.get(1)?,
        native: r.get(2)?,
    })
}

fn display_title(session: &Session, name: Option<&str>) -> String {
    name.filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            format!(
                "[{}] {}",
                session.agent,
                session.native.chars().take(8).collect::<String>()
            )
        })
}
fn queue_title(db: &Connection, target: &Conversation, title: &str) -> Result<()> {
    db.execute("INSERT INTO conversation_titles(platform,space,thread,desired) VALUES(?1,?2,?3,?4) ON CONFLICT(platform,space,thread) DO UPDATE SET desired=excluded.desired,next_attempt=CASE WHEN desired<>excluded.desired THEN 0 ELSE next_attempt END",params![target.platform,target.space,target.thread,title])?;
    Ok(())
}
