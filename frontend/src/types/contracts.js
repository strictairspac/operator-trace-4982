/** @typedef {{id:number,name:string,sensors:object[],readings:object[],purge_status:'active'|'purging'|'purge_failed'|'purged',purge_error?:string|null}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[]}} Plan */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null,purge_job_id?:number|null,snapshot:object}} Trace */
/** @typedef {{id:number,dataset_id:number,status:'queued'|'running'|'succeeded'|'failed',attempts:number,error?:string|null}} PurgeJob */
export {};
