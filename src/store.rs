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
            tables == 0 || version == 2,
            "Unsupported uaoi database. Remove old uaoi state before starting."
        );
        conn.pragma_update(None, "user_version", 2)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, project TEXT NOT NULL, agent TEXT NOT NULL, native TEXT NOT NULL, UNIQUE(agent,native));
          CREATE TABLE IF NOT EXISTS bindings(platform TEXT, space TEXT, thread TEXT, session TEXT REFERENCES sessions(id), PRIMARY KEY(platform,space,thread));
          CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, state TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
          CREATE TABLE IF NOT EXISTS submissions(id TEXT PRIMARY KEY, session TEXT, text TEXT, mode TEXT, state TEXT);
          CREATE TABLE IF NOT EXISTS events(session TEXT, key TEXT, body TEXT, PRIMARY KEY(session,key));
          CREATE TABLE IF NOT EXISTS deliveries(id INTEGER PRIMARY KEY, platform TEXT, space TEXT, thread TEXT, key TEXT, text TEXT, message TEXT, buttons TEXT, state TEXT DEFAULT 'pending', UNIQUE(platform,space,thread,key));
          CREATE TABLE IF NOT EXISTS session_titles(session TEXT PRIMARY KEY REFERENCES sessions(id),title TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS conversation_titles(platform TEXT,space TEXT,thread TEXT,desired TEXT NOT NULL,applied TEXT,next_attempt INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(platform,space,thread));
          CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, session TEXT, body TEXT, resolved INTEGER DEFAULT 0);
")?;
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
    pub async fn ensure_session(
        &self,
        project: &str,
        agent: &str,
        native: &str,
    ) -> Result<Session> {
        let (p, a, n) = (project.to_owned(), agent.to_owned(), native.to_owned());
        self.call(move |db| {
            db.execute(
                "INSERT OR IGNORE INTO sessions VALUES(?1,?2,?3,?4)",
                params![uuid::Uuid::new_v4().to_string(), p, a, n],
            )?;
            let s = db.query_row(
                "SELECT id,project,agent,native FROM sessions WHERE agent=?1 AND native=?2",
                params![a, n],
                row_session,
            )?;
            anyhow::ensure!(
                s.project == p,
                "Thread belongs to project {} (session {})",
                s.project,
                s.id
            );
            Ok(s)
        })
        .await
    }
    pub async fn session(&self, id: &str) -> Result<Session> {
        let id = id.to_owned();
        self.call(move |db| {
            Ok(db.query_row(
                "SELECT id,project,agent,native FROM sessions WHERE id=?1",
                [id],
                row_session,
            )?)
        })
        .await
    }
    pub async fn sessions(&self) -> Result<Vec<Session>> {
        self.call(|db| {
            Ok(db
                .prepare("SELECT id,project,agent,native FROM sessions ORDER BY rowid")?
                .query_map([], row_session)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
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
            tx.execute(
                "INSERT OR IGNORE INTO bindings VALUES(?1,?2,?3,?4)",
                params![t.platform, t.space, t.thread, s],
            )?;
            let session = tx.query_row(
                "SELECT id,project,agent,native FROM sessions WHERE id=?1",
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
            tx.commit()?;
            Ok(())
        })
        .await
    }
    pub async fn bound(&self, target: &Conversation) -> Result<Option<Session>> {
        let t = target.clone();
        self.call(move|db|Ok(db.query_row("SELECT s.id,s.project,s.agent,s.native FROM sessions s JOIN bindings b ON b.session=s.id WHERE platform=?1 AND space=?2 AND thread=?3",params![t.platform,t.space,t.thread],row_session).optional()?)).await
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
    pub async fn native_session(&self, agent: &str, native: &str) -> Result<Option<Session>> {
        let (agent, native) = (agent.to_owned(), native.to_owned());
        self.call(move |db| {
            Ok(db
                .query_row(
                    "SELECT id,project,agent,native FROM sessions WHERE agent=?1 AND native=?2",
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
        self.call(|db|Ok(db.prepare("SELECT platform,space,thread,desired FROM conversation_titles WHERE (applied IS NULL OR applied<>desired) AND next_attempt<=unixepoch() ORDER BY rowid LIMIT 25")?.query_map([],|r|Ok(TitleChange{target:Conversation{platform:r.get(0)?,space:r.get(1)?,thread:r.get(2)?},title:r.get(3)?}))?.collect::<rusqlite::Result<Vec<_>>>()?)).await
    }
    pub async fn title_is_current(&self, change: &TitleChange) -> Result<bool> {
        let c = change.clone();
        self.call(move |db|Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM conversation_titles WHERE platform=?1 AND space=?2 AND thread=?3 AND desired=?4 AND (applied IS NULL OR applied<>desired) AND next_attempt<=unixepoch())",params![c.target.platform,c.target.space,c.target.thread,c.title],|r|r.get(0))?)).await
    }
    pub async fn title_applied(&self, change: &TitleChange) -> Result<()> {
        let c = change.clone();
        self.call(move |db| {
            // Record what actually reached the platform, even if a newer name arrived in flight.
            db.execute("UPDATE conversation_titles SET applied=?4 WHERE platform=?1 AND space=?2 AND thread=?3",params![c.target.platform,c.target.space,c.target.thread,c.title])?;Ok(())
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
    pub async fn submission(
        &self,
        id: &str,
        session: &str,
        text: &str,
        steer: bool,
        state: &str,
    ) -> Result<()> {
        let (id, session, text, state) = (
            id.to_owned(),
            session.to_owned(),
            text.to_owned(),
            state.to_owned(),
        );
        self.call(move |db| {
            db.execute("INSERT INTO submissions VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET state=CASE WHEN submissions.state='consumed' THEN 'consumed' ELSE excluded.state END",params![id,session,text,if steer {"steer"}else{"queue"},state])?;
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
            let rows=db.prepare("SELECT id,text,mode,state FROM submissions WHERE session=?1 AND state IN ('unknown','submitted') ORDER BY rowid")?.query_map([session],|r|Ok(json!({"id":r.get::<_,String>(0)?,"text":r.get::<_,String>(1)?,"mode":r.get::<_,String>(2)?,"state":r.get::<_,String>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
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
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<(String, String)>,
    ) -> Result<()> {
        let (t, k, txt) = (target.clone(), key.to_owned(), text.to_owned());
        self.call(move|db|{
            db.execute("INSERT INTO deliveries(platform,space,thread,key,text,buttons) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(platform,space,thread,key) DO UPDATE SET text=excluded.text,buttons=excluded.buttons,state=CASE WHEN deliveries.state='sent' AND (deliveries.text<>excluded.text OR deliveries.buttons<>excluded.buttons) THEN 'pending' ELSE deliveries.state END",params![t.platform,t.space,t.thread,k,txt,json!(buttons).to_string()])?; Ok(())
        }).await
    }
    pub async fn pending(&self) -> Result<Vec<Delivery>> {
        self.call(|db|{
            let rows=db.prepare("SELECT id,platform,space,thread,text,message,buttons FROM deliveries WHERE state='pending' ORDER BY id LIMIT 25")?.query_map([],|r|Ok((r.get::<_,i64>(0)?,Conversation{platform:r.get(1)?,space:r.get(2)?,thread:r.get(3)?},r.get::<_,String>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,String>(6)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|(id,target,text,message,b)|Ok(Delivery{id,target,text,message,buttons:serde_json::from_str(&b)?})).collect()
        }).await
    }
    pub async fn delivery(&self, d: &Delivery, state: &str, message: Option<String>) -> Result<()> {
        let (id, s, txt) = (d.id, state.to_owned(), d.text.clone());
        self.call(move|db| {
            db.execute("UPDATE deliveries SET state=CASE WHEN ?2='sent' AND text<>?4 THEN 'pending' ELSE ?2 END,message=COALESCE(?3,message) WHERE id=?1",params![id,s,message,txt])?; Ok(())
        }).await
    }
}
fn row_session(r: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        id: r.get(0)?,
        project: r.get(1)?,
        agent: r.get(2)?,
        native: r.get(3)?,
    })
}

fn display_title(session: &Session, name: Option<&str>) -> String {
    name.filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            format!(
                "[{}] {}",
                session.project,
                session.native.chars().take(8).collect::<String>()
            )
        })
}
fn queue_title(db: &Connection, target: &Conversation, title: &str) -> Result<()> {
    db.execute("INSERT INTO conversation_titles(platform,space,thread,desired) VALUES(?1,?2,?3,?4) ON CONFLICT(platform,space,thread) DO UPDATE SET desired=excluded.desired,next_attempt=CASE WHEN desired<>excluded.desired THEN 0 ELSE next_attempt END",params![target.platform,target.space,target.thread,title])?;
    Ok(())
}
