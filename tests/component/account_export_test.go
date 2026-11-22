//go:build component

package component

import (
	"encoding/json"
	"strings"
 "testing"
 "time"
 "github.com/google/uuid"
 postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
 "github.com/overmindv/tasks/internal/domain"
)

func TestAccountExportEscapedWireSize(t *testing.T) {
 pool:=openComponentDB(t);ctx:=t.Context();store:=postgresadapter.New(pool)
 owner,task,version:=uuid.New(),uuid.New(),uuid.New()
 if _,err:=pool.Exec(ctx,`INSERT INTO tasks(id,created_by,updated_by)VALUES($1,$2,$2)`,task,owner);err!=nil{t.Fatal(err)}
 if _,err:=pool.Exec(ctx,`INSERT INTO task_versions(id,task_id,version_number,title,statement,task_type,difficulty,created_by)VALUES($1,$2,1,'Wire size','Fixture','single_choice','easy',$3)`,version,task,uuid.New());err!=nil{t.Fatal(err)}
 for range 2{if _,err:=pool.Exec(ctx,`INSERT INTO code_submissions(id,user_id,task_id,task_version_id,execution_id,correlation_id,idempotency_key,request_hash,language,source_file_name,source_code)VALUES(gen_random_uuid(),$1,$2,$3,gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),'fixture','python','main.py',$4)`,owner,task,version,strings.Repeat("<",256<<10));err!=nil{t.Fatal(err)}}
 page,err:=store.AccountExport(ctx,owner,"","",100);if err!=nil{t.Fatal(err)}
 raw,err:=json.Marshal(page);if err!=nil{t.Fatal(err)}
 if len(raw)>2<<20||len(page.Records)!=1||page.NextCursor==""{t.Fatalf("wire page exceeded budget: bytes=%d records=%d cursor=%s",len(raw),len(page.Records),page.NextCursor)}
 next,err:=store.AccountExport(ctx,owner,page.NextCursor,page.Watermark,100);if err!=nil||len(next.Records)!=1||next.NextCursor!=""{t.Fatal("continuation",next.NextCursor,err)}
}

func TestAccountExportStableSubjectAndDeletion(t *testing.T){
 pool:=openComponentDB(t);ctx:=t.Context();store:=postgresadapter.New(pool)
 owner,other,task,version,attempt:=uuid.New(),uuid.New(),uuid.New(),uuid.New(),uuid.New()
 for _,q:=range []struct{sql string;args []any}{
 {`INSERT INTO tasks(id,created_by,updated_by)VALUES($1,$2,$2)`,[]any{task,owner}},
 {`INSERT INTO task_versions(id,task_id,version_number,title,statement,task_type,difficulty,created_by)VALUES($1,$2,1,'Own title','Own statement','single_choice','easy',$3)`,[]any{version,task,owner}},
 {`INSERT INTO submissions(id,user_id,task_id,task_version_id,idempotency_key,request_hash,verdict)VALUES($1,$2,$3,$4,gen_random_uuid(),'fixture','accepted')`,[]any{attempt,owner,task,version}},
 }{if _,err:=pool.Exec(ctx,q.sql,q.args...);err!=nil{t.Fatal(err)}}
 page,err:=store.AccountExport(ctx,owner,"","",1);if err!=nil||len(page.Records)!=1||page.NextCursor==""{t.Fatalf("first page: %+v %v",page,err)}
 if _,err=pool.Exec(ctx,`UPDATE task_versions SET statement='Later edit' WHERE id=$1`,version);err!=nil{t.Fatal(err)}
 second,err:=store.AccountExport(ctx,owner,page.NextCursor,page.Watermark,1);if err!=nil||len(second.Records)!=1||second.Records[0].Text!="Own statement"||second.NextCursor!=""{t.Fatalf("stable author page %+v %v",second,err)}
 if _,err=store.AccountExport(ctx,other,"",page.Watermark,100);err==nil{t.Fatal("foreign snapshot accepted")}
 done,err:=store.ApplyAccountDeletion(ctx,domain.AccountDeletion{EventID:uuid.New(),UserID:owner,Version:1},100);if err!=nil||!done{t.Fatal("delete",done,err)}
 if _,err=store.AccountExport(ctx,owner,"",page.Watermark,100);err==nil{t.Fatal("deleted snapshot accepted")}
 var author,editor uuid.UUID;if err=pool.QueryRow(ctx,`SELECT created_by,updated_by FROM tasks WHERE id=$1`,task).Scan(&author,&editor);err!=nil||author!=uuid.Nil||editor!=uuid.Nil{t.Fatal("author retained",author,editor,err)}
 if _,err=pool.Exec(ctx,`UPDATE tasks SET updated_by=$2 WHERE id=$1`,task,owner);err==nil{t.Fatal("deleted editor resurrected")}
}

func TestRawExpiryAndHistoryExpiryAreSeparate(t *testing.T){
 pool:=openComponentDB(t);ctx:=t.Context();store:=postgresadapter.New(pool)
 owner,task,version,code:=uuid.New(),uuid.New(),uuid.New(),uuid.New()
 for _,q:=range []struct{sql string;args []any}{
 {`INSERT INTO tasks(id,created_by,updated_by)VALUES($1,$2,$2)`,[]any{task,owner}},
 {`INSERT INTO task_versions(id,task_id,version_number,title,statement,task_type,difficulty,created_by)VALUES($1,$2,1,'Expiry','Fixture','single_choice','easy',$3)`,[]any{version,task,owner}},
 {`INSERT INTO code_submissions(id,user_id,task_id,task_version_id,execution_id,correlation_id,idempotency_key,request_hash,language,source_file_name,source_code,status,verdict,completed_at,created_at,execution_result)VALUES($1,$2,$3,$4,gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),'fixture','python','main.py','secret code','completed','accepted',now()-interval '2 day',now()-interval '2 day','{"stdout":"private output"}')`,[]any{code,owner,task,version}},
 }{if _,err:=pool.Exec(ctx,q.sql,q.args...);err!=nil{t.Fatal(err)}}
 if err:=store.ExpireLearningData(ctx,time.Time{},time.Time{},100);err!=nil{t.Fatal(err)}
 var source *string;if err:=pool.QueryRow(ctx,`SELECT source_code FROM code_submissions WHERE id=$1`,code).Scan(&source);err!=nil||source==nil{t.Fatal("disabled expiry modified data",err)}
 before,err:=store.OutcomeManifest(ctx,0,0);if err!=nil{t.Fatal(err)}
 if err=store.ExpireLearningData(ctx,time.Now().Add(-time.Hour),time.Time{},100);err!=nil{t.Fatal(err)}
 if err=pool.QueryRow(ctx,`SELECT source_code FROM code_submissions WHERE id=$1`,code).Scan(&source);err!=nil||source!=nil{t.Fatal("raw code survived",err)}
 if _,err=store.OutcomeReplay(ctx,before.Watermark,before.PrivacyEpoch,0,100);err==nil{t.Fatal("stale epoch accepted")}
 if err=store.ExpireLearningData(ctx,time.Time{},time.Now().Add(-time.Hour),100);err!=nil{t.Fatal(err)}
 var count int;if err=pool.QueryRow(ctx,`SELECT count(*) FROM code_submissions WHERE id=$1`,code).Scan(&count);err!=nil||count!=0{t.Fatal("history survived",err)}
 if err=pool.QueryRow(ctx,`SELECT count(*) FROM outcome_retired_aggregates WHERE aggregate_id=$1`,code).Scan(&count);err!=nil||count!=1{t.Fatal("missing resurrection fence",err)}
}
