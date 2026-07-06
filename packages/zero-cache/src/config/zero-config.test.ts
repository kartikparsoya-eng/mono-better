import {stripVTControlCharacters as stripAnsi} from 'node:util';
import {expect, test, vi} from 'vitest';
import {
  parseOptions,
  parseOptionsAdvanced,
} from '../../../shared/src/options.ts';
import {INVALID_APP_ID_MESSAGE} from '../types/shards.ts';
import {zeroOptions} from './zero-config.ts';

class ExitAfterUsage extends Error {}
const exit = () => {
  throw new ExitAfterUsage();
};

// Tip: Rerun tests with -u to update the snapshot.
test('zero-cache --help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(zeroOptions, {
      argv: ['--help'],
      envNamePrefix: 'ZERO_',
      env: {},
      logger,
      exit,
    }),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --upstream-db string                                               required                                                                                                                   
       ZERO_UPSTREAM_DB env                                                                                                                                                                        
                                                                        The "upstream" authoritative postgres database.                                                                            
                                                                        In the future we will support other types of upstream besides PG.                                                          
                                                                                                                                                                                                   
     --upstream-max-conns number                                        default: 20                                                                                                                
       ZERO_UPSTREAM_MAX_CONNS env                                                                                                                                                                 
                                                                        The maximum number of connections to open to the upstream database                                                         
                                                                        for committing mutations. This is divided evenly amongst sync workers.                                                     
                                                                        In addition to this number, zero-cache uses one connection for the                                                         
                                                                        replication stream.                                                                                                        
                                                                                                                                                                                                   
                                                                        Note that this number must allow for at least one connection per                                                           
                                                                        sync worker, or zero-cache will fail to start. See num-sync-workers                                                        
                                                                                                                                                                                                   
     --upstream-pg-replication-slot-failover boolean                    optional                                                                                                                   
       ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER env                                                                                                                                              
                                                                        For upstream Postgres versions 17+, creates replication slots with the                                                     
                                                                        failover parameter set to true to enable slot synchronization                                                              
                                                                        and failover. Note that additional Postgres-level configuration is necessary                                               
                                                                        when enabling this option. For details, see:                                                                               
                                                                                                                                                                                                   
                                                                        https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION 
                                                                                                                                                                                                   
                                                                        (Note that this option has no effect for Postgres versions before 17.)                                                     
                                                                                                                                                                                                   
     --mutate-url string[]                                              optional                                                                                                                   
       ZERO_MUTATE_URL env                                                                                                                                                                         
                                                                        The URL of the API server to which zero-cache will push mutations.                                                         
                                                                                                                                                                                                   
                                                                        IMPORTANT: URLs are matched using URLPattern, a standard Web API.                                                          
                                                                                                                                                                                                   
                                                                        Pattern Syntax:                                                                                                            
                                                                          URLPattern uses a simple and intuitive syntax similar to Express routes.                                                 
                                                                          Wildcards and named parameters make it easy to match multiple URLs.                                                      
                                                                                                                                                                                                   
                                                                        Basic Examples:                                                                                                            
                                                                          Exact URL match:                                                                                                         
                                                                            "https://api.example.com/mutate"                                                                                       
                                                                                                                                                                                                   
                                                                          Any subdomain using wildcard:                                                                                            
                                                                            "https://*.example.com/mutate"                                                                                         
                                                                                                                                                                                                   
                                                                          Multiple subdomain levels:                                                                                               
                                                                            "https://*.*.example.com/mutate"                                                                                       
                                                                                                                                                                                                   
                                                                          Any path under a domain:                                                                                                 
                                                                            "https://api.example.com/*"                                                                                            
                                                                                                                                                                                                   
                                                                          Named path parameters:                                                                                                   
                                                                            "https://api.example.com/:version/mutate"                                                                              
                                                                            ↳ Matches "https://api.example.com/v1/mutate", "https://api.example.com/v2/mutate", etc.                               
                                                                                                                                                                                                   
                                                                        Advanced Patterns:                                                                                                         
                                                                          Optional path segments:                                                                                                  
                                                                            "https://api.example.com/:path?"                                                                                       
                                                                                                                                                                                                   
                                                                          Regex in segments (for specific patterns):                                                                               
                                                                            "https://api.example.com/:version(v\\d+)/mutate"                                                                        
                                                                            ↳ Matches only "v" followed by digits                                                                                  
                                                                                                                                                                                                   
                                                                        Multiple patterns:                                                                                                         
                                                                          ["https://api1.example.com/mutate", "https://api2.example.com/mutate"]                                                   
                                                                                                                                                                                                   
                                                                        Note: Query parameters and URL fragments (#) are automatically ignored during matching.                                    
                                                                                                                                                                                                   
                                                                        For full URLPattern syntax, see: https://developer.mozilla.org/en-US/docs/Web/API/URLPattern                               
                                                                                                                                                                                                   
     --mutate-api-key string                                            optional                                                                                                                   
       ZERO_MUTATE_API_KEY env                                                                                                                                                                     
                                                                        An optional secret used to authorize zero-cache to call the API server handling writes.                                    
                                                                                                                                                                                                   
     --mutate-forward-cookies boolean                                   default: false                                                                                                             
       ZERO_MUTATE_FORWARD_COOKIES env                                                                                                                                                             
                                                                        If true, zero-cache will forward cookies from the request.                                                                 
                                                                        This is useful for passing authentication cookies to the API server.                                                       
                                                                        If false, cookies are not forwarded.                                                                                       
                                                                                                                                                                                                   
     --mutate-allowed-client-headers string[]                           optional                                                                                                                   
       ZERO_MUTATE_ALLOWED_CLIENT_HEADERS env                                                                                                                                                      
                                                                        A list of header names that clients are allowed to set via custom headers.                                                 
                                                                        If specified, only headers in this list will be forwarded to the push URL.                                                 
                                                                        Header names are case-insensitive.                                                                                         
                                                                        If not specified, no client-provided headers are forwarded (secure by default).                                            
                                                                        Example: ZERO_MUTATE_ALLOWED_CLIENT_HEADERS=x-request-id,x-correlation-id                                                  
                                                                                                                                                                                                   
     --query-url string[]                                               optional                                                                                                                   
       ZERO_QUERY_URL env                                                                                                                                                                          
                                                                        The URL of the API server to which zero-cache will send synced queries.                                                    
                                                                                                                                                                                                   
                                                                        IMPORTANT: URLs are matched using URLPattern, a standard Web API.                                                          
                                                                                                                                                                                                   
                                                                        Pattern Syntax:                                                                                                            
                                                                          URLPattern uses a simple and intuitive syntax similar to Express routes.                                                 
                                                                          Wildcards and named parameters make it easy to match multiple URLs.                                                      
                                                                                                                                                                                                   
                                                                        Basic Examples:                                                                                                            
                                                                          Exact URL match:                                                                                                         
                                                                            "https://api.example.com/mutate"                                                                                       
                                                                                                                                                                                                   
                                                                          Any subdomain using wildcard:                                                                                            
                                                                            "https://*.example.com/mutate"                                                                                         
                                                                                                                                                                                                   
                                                                          Multiple subdomain levels:                                                                                               
                                                                            "https://*.*.example.com/mutate"                                                                                       
                                                                                                                                                                                                   
                                                                          Any path under a domain:                                                                                                 
                                                                            "https://api.example.com/*"                                                                                            
                                                                                                                                                                                                   
                                                                          Named path parameters:                                                                                                   
                                                                            "https://api.example.com/:version/mutate"                                                                              
                                                                            ↳ Matches "https://api.example.com/v1/mutate", "https://api.example.com/v2/mutate", etc.                               
                                                                                                                                                                                                   
                                                                        Advanced Patterns:                                                                                                         
                                                                          Optional path segments:                                                                                                  
                                                                            "https://api.example.com/:path?"                                                                                       
                                                                                                                                                                                                   
                                                                          Regex in segments (for specific patterns):                                                                               
                                                                            "https://api.example.com/:version(v\\d+)/mutate"                                                                        
                                                                            ↳ Matches only "v" followed by digits                                                                                  
                                                                                                                                                                                                   
                                                                        Multiple patterns:                                                                                                         
                                                                          ["https://api1.example.com/mutate", "https://api2.example.com/mutate"]                                                   
                                                                                                                                                                                                   
                                                                        Note: Query parameters and URL fragments (#) are automatically ignored during matching.                                    
                                                                                                                                                                                                   
                                                                        For full URLPattern syntax, see: https://developer.mozilla.org/en-US/docs/Web/API/URLPattern                               
                                                                                                                                                                                                   
     --query-api-key string                                             optional                                                                                                                   
       ZERO_QUERY_API_KEY env                                                                                                                                                                      
                                                                        An optional secret used to authorize zero-cache to call the API server handling writes.                                    
                                                                                                                                                                                                   
     --query-forward-cookies boolean                                    default: false                                                                                                             
       ZERO_QUERY_FORWARD_COOKIES env                                                                                                                                                              
                                                                        If true, zero-cache will forward cookies from the request.                                                                 
                                                                        This is useful for passing authentication cookies to the API server.                                                       
                                                                        If false, cookies are not forwarded.                                                                                       
                                                                                                                                                                                                   
     --query-allowed-client-headers string[]                            optional                                                                                                                   
       ZERO_QUERY_ALLOWED_CLIENT_HEADERS env                                                                                                                                                       
                                                                        A list of header names that clients are allowed to set via custom headers.                                                 
                                                                        If specified, only headers in this list will be forwarded to the query URL.                                                
                                                                        Header names are case-insensitive.                                                                                         
                                                                        If not specified, no client-provided headers are forwarded (secure by default).                                            
                                                                        Example: ZERO_QUERY_ALLOWED_CLIENT_HEADERS=x-request-id,x-correlation-id                                                   
                                                                                                                                                                                                   
     --enable-crud-mutations boolean                                    default: true                                                                                                              
       ZERO_ENABLE_CRUD_MUTATIONS env                                                                                                                                                              
                                                                        Enables support for legacy CRUD mutations. When this is false, no connections                                              
                                                                        are made from view-syncers to the upstream db, and push messages with CRUD mutations                                       
                                                                        result in an InvalidPush response.                                                                                         
                                                                                                                                                                                                   
     --cvr-db string                                                    optional                                                                                                                   
       ZERO_CVR_DB env                                                                                                                                                                             
                                                                        The Postgres database used to store CVRs. CVRs (client view records) keep track                                            
                                                                        of the data synced to clients in order to determine the diff to send on reconnect.                                         
                                                                        If unspecified, the upstream-db will be used.                                                                              
                                                                                                                                                                                                   
     --cvr-max-conns number                                             default: 30                                                                                                                
       ZERO_CVR_MAX_CONNS env                                                                                                                                                                      
                                                                        The maximum number of connections to open to the CVR database.                                                             
                                                                        This is divided evenly amongst sync workers.                                                                               
                                                                                                                                                                                                   
                                                                        Note that this number must allow for at least one connection per                                                           
                                                                        sync worker, or zero-cache will fail to start. See num-sync-workers                                                        
                                                                                                                                                                                                   
     --cvr-garbage-collection-inactivity-threshold-hours number         default: 48                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS env                                                                                                                                  
                                                                        The duration after which an inactive CVR is eligible for garbage collection.                                               
                                                                        Note that garbage collection is an incremental, periodic process which does not                                            
                                                                        necessarily purge all eligible CVRs immediately.                                                                           
                                                                                                                                                                                                   
     --cvr-garbage-collection-initial-interval-seconds number           default: 60                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS env                                                                                                                                    
                                                                        The initial interval at which to check and garbage collect inactive CVRs.                                                  
                                                                        This interval is increased exponentially (up to 16 minutes) when there is                                                  
                                                                        nothing to purge.                                                                                                          
                                                                                                                                                                                                   
     --cvr-garbage-collection-initial-batch-size number                 default: 25                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE env                                                                                                                                          
                                                                        The initial number of CVRs to purge per garbage collection interval.                                                       
                                                                        This number is increased linearly if the rate of new CVRs exceeds the rate of                                              
                                                                        purged CVRs, in order to reach a steady state.                                                                             
                                                                                                                                                                                                   
                                                                        Setting this to 0 effectively disables CVR garbage collection.                                                             
                                                                                                                                                                                                   
     --query-hydration-stats boolean                                    optional                                                                                                                   
       ZERO_QUERY_HYDRATION_STATS env                                                                                                                                                              
                                                                        Track and log the number of rows considered by query hydrations which                                                      
                                                                        take longer than log-slow-hydrate-threshold milliseconds.                                                                  
                                                                        This is useful for debugging and performance tuning.                                                                       
                                                                                                                                                                                                   
     --enable-query-planner boolean                                     default: true                                                                                                              
       ZERO_ENABLE_QUERY_PLANNER env                                                                                                                                                               
                                                                        Enable the query planner for optimizing ZQL queries.                                                                       
                                                                                                                                                                                                   
                                                                        The query planner analyzes and optimizes query execution by determining                                                    
                                                                        the most efficient join strategies.                                                                                        
                                                                                                                                                                                                   
                                                                        You can disable the planner if it is picking bad strategies.                                                               
                                                                                                                                                                                                   
     --yield-threshold-ms number                                        default: 10                                                                                                                
       ZERO_YIELD_THRESHOLD_MS env                                                                                                                                                                 
                                                                        The maximum amount of time in milliseconds that a sync worker will                                                         
                                                                        spend in IVM (processing query hydration and advancement) before yielding                                                  
                                                                        to the event loop. Lower values increase responsiveness and fairness at                                                    
                                                                        the cost of reduced throughput.                                                                                            
                                                                                                                                                                                                   
     --change-db string                                                 optional                                                                                                                   
       ZERO_CHANGE_DB env                                                                                                                                                                          
                                                                        The Postgres database used to store recent replication log entries, in order                                               
                                                                        to sync multiple view-syncers without requiring multiple replication slots on                                              
                                                                        the upstream database. If unspecified, the upstream-db will be used.                                                       
                                                                                                                                                                                                   
     --change-max-conns number                                          default: 5                                                                                                                 
       ZERO_CHANGE_MAX_CONNS env                                                                                                                                                                   
                                                                        The maximum number of connections to open to the change database.                                                          
                                                                        This is used by the change-streamer for catching up                                                                        
                                                                        zero-cache replication subscriptions.                                                                                      
                                                                                                                                                                                                   
     --replica-file string                                              default: "zero.db"                                                                                                         
       ZERO_REPLICA_FILE env                                                                                                                                                                       
                                                                        File path to the SQLite replica that zero-cache maintains.                                                                 
                                                                        This can be lost, but if it is, zero-cache will have to re-replicate next                                                  
                                                                        time it starts up.                                                                                                         
                                                                                                                                                                                                   
     --replica-vacuum-interval-hours number                             optional                                                                                                                   
       ZERO_REPLICA_VACUUM_INTERVAL_HOURS env                                                                                                                                                      
                                                                        Performs a VACUUM at server startup if the specified number of hours has elapsed                                           
                                                                        since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight                                               
                                                                        and requires double the size of the db in disk space. If unspecified, VACUUM                                               
                                                                        operations are not performed.                                                                                              
                                                                                                                                                                                                   
     --log-level debug,info,warn,error                                  default: "info"                                                                                                            
       ZERO_LOG_LEVEL env                                                                                                                                                                          
                                                                                                                                                                                                   
     --log-format text,json                                             default: "text"                                                                                                            
       ZERO_LOG_FORMAT env                                                                                                                                                                         
                                                                        Use text for developer-friendly console logging                                                                            
                                                                        and json for consumption by structured-logging services                                                                    
                                                                                                                                                                                                   
     --log-slow-row-threshold number                                    default: 2                                                                                                                 
       ZERO_LOG_SLOW_ROW_THRESHOLD env                                                                                                                                                             
                                                                        The number of ms a row must take to fetch from table-source before it is considered slow.                                  
                                                                                                                                                                                                   
     --log-slow-hydrate-threshold number                                default: 100                                                                                                               
       ZERO_LOG_SLOW_HYDRATE_THRESHOLD env                                                                                                                                                         
                                                                        The number of milliseconds a query hydration must take to print a slow warning.                                            
                                                                                                                                                                                                   
     --log-ivm-sampling number                                          default: 5000                                                                                                              
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                                                   
                                                                        How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.                               
                                                                                                                                                                                                   
     --app-id string                                                    default: "zero"                                                                                                            
       ZERO_APP_ID env                                                                                                                                                                             
                                                                        Unique identifier for the app.                                                                                             
                                                                                                                                                                                                   
                                                                        Multiple zero-cache apps can run on a single upstream database, each of which                                              
                                                                        is isolated from the others, with its own permissions, sharding (future feature),                                          
                                                                        and change/cvr databases.                                                                                                  
                                                                                                                                                                                                   
                                                                        The metadata of an app is stored in an upstream schema with the same name,                                                 
                                                                        e.g. "zero", and the metadata for each app shard, e.g. client and mutation                                                 
                                                                        ids, is stored in the "{app-id}_{#}" schema. (Currently there is only a single                                             
                                                                        "0" shard, but this will change with sharding).                                                                            
                                                                                                                                                                                                   
                                                                        The CVR and Change data are managed in schemas named "{app-id}_{shard-num}/cvr"                                            
                                                                        and "{app-id}_{shard-num}/cdc", respectively, allowing multiple apps and shards                                            
                                                                        to share the same database instance (e.g. a Postgres "cluster") for CVR and Change management.                             
                                                                                                                                                                                                   
                                                                        Due to constraints on replication slot names, an App ID may only consist of                                                
                                                                        lower-case letters, numbers, and the underscore character.                                                                 
                                                                                                                                                                                                   
                                                                        Note that this option is used by both zero-cache and zero-deploy-permissions.                                              
                                                                                                                                                                                                   
     --app-publications string[]                                        default: []                                                                                                                
       ZERO_APP_PUBLICATIONS env                                                                                                                                                                   
                                                                        Postgres PUBLICATIONs that define the tables and columns to                                                                
                                                                        replicate. Publication names may not begin with an underscore,                                                             
                                                                        as zero reserves that prefix for internal use.                                                                             
                                                                                                                                                                                                   
                                                                        If unspecified, zero-cache will create and use an internal publication that                                                
                                                                        publishes all tables in the public schema, i.e.:                                                                           
                                                                                                                                                                                                   
                                                                        CREATE PUBLICATION _{app-id}_public_0 FOR TABLES IN SCHEMA public;                                                         
                                                                                                                                                                                                   
                                                                        Note that changing the set of publications will result in resyncing the replica,                                           
                                                                        which may involve downtime (replication lag) while the new replica is initializing.                                        
                                                                        To change the set of publications without disrupting an existing app, a new app                                            
                                                                        should be created.                                                                                                         
                                                                                                                                                                                                   
     --auth-revalidate-interval-seconds number                          default: 300                                                                                                               
       ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS env                                                                                                                                                   
                                                                        The interval in seconds between periodic /query auth revalidation for validated connections.                               
                                                                        If unset, periodic auth revalidation is disabled.                                                                          
                                                                                                                                                                                                   
     --auth-retransform-interval-seconds number                         default: 300                                                                                                               
       ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS env                                                                                                                                                  
                                                                        The interval in seconds between periodic shared /query retransform work for a client group.                                
                                                                        If unset, periodic shared retransform is disabled.                                                                         
                                                                                                                                                                                                   
     --port number                                                      default: 4848                                                                                                              
       ZERO_PORT env                                                                                                                                                                               
                                                                        The port for sync connections.                                                                                             
                                                                                                                                                                                                   
     --keepalive-timeout-ms number                                      optional                                                                                                                   
       ZERO_KEEPALIVE_TIMEOUT_MS env                                                                                                                                                               
                                                                        The timeout since the last /keepalive request after which the server will initiate                                         
                                                                        a graceful shutdown. This is a workaround for AWS Elastic Container Service, which                                         
                                                                        otherwise provides no signal that a target has been deregistered (and should thus begin                                    
                                                                        shutdown); the cessation of health checks at /keepalive is instead used as the signal to                                   
                                                                        drain. (ECS later sends a SIGTERM before killing the server but only allows a 30-second                                    
                                                                        timeout before sending SIGKILL).                                                                                           
                                                                                                                                                                                                   
                                                                        Other container runners explicitly send a SIGTERM followed by a configurable drain interval,                               
                                                                        in which case /keepalive logic is not necessary.                                                                           
                                                                                                                                                                                                   
                                                                        When running the server in ECS, this timeout should be set to some multiple of the health                                  
                                                                        check interval. If the option is unset, the keepalive timeout is disabled in non-ECS environments,                         
                                                                        and defaults to 20 seconds when run in ECS (determined by the presence of the                                              
                                                                        ECS_CONTAINER_METADATA_URI_V4 environment variable as per                                                                  
                                                                        https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-environment-variables.html).                               
                                                                                                                                                                                                   
     --change-streamer-uri string                                       optional                                                                                                                   
       ZERO_CHANGE_STREAMER_URI env                                                                                                                                                                
                                                                        When set, connects to the change-streamer at the given URI.                                                                
                                                                        In a multi-node setup, this should be specified in view-syncer options,                                                    
                                                                        pointing to the replication-manager URI, which runs a change-streamer                                                      
                                                                        on port 4849.                                                                                                              
                                                                                                                                                                                                   
     --change-streamer-mode dedicated,discover                          default: "dedicated"                                                                                                       
       ZERO_CHANGE_STREAMER_MODE env                                                                                                                                                               
                                                                        As an alternative to ZERO_CHANGE_STREAMER_URI, the ZERO_CHANGE_STREAMER_MODE                                               
                                                                        can be set to "discover" to instruct the view-syncer to connect to the                                                     
                                                                        ip address registered by the replication-manager upon startup.                                                             
                                                                                                                                                                                                   
                                                                        This may not work in all networking configurations, e.g. certain private                                                   
                                                                        networking or port forwarding configurations. Using the ZERO_CHANGE_STREAMER_URI                                           
                                                                        with an explicit routable hostname is recommended instead.                                                                 
                                                                                                                                                                                                   
                                                                        Note: This option is ignored if the ZERO_CHANGE_STREAMER_URI is set.                                                       
                                                                                                                                                                                                   
     --change-streamer-port number                                      optional                                                                                                                   
       ZERO_CHANGE_STREAMER_PORT env                                                                                                                                                               
                                                                        The port on which the change-streamer runs. This is an internal                                                            
                                                                        protocol between the replication-manager and view-syncers, which                                                           
                                                                        runs in the same process tree in local development or a single-node configuration.                                         
                                                                                                                                                                                                   
                                                                        If unspecified, defaults to --port + 1.                                                                                    
                                                                                                                                                                                                   
     --change-streamer-startup-delay-ms number                          default: 15000                                                                                                             
       ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS env                                                                                                                                                   
                                                                        The delay to wait before the change-streamer takes over the replication stream                                             
                                                                        (i.e. the handoff during replication-manager updates), to allow loadbalancers to register                                  
                                                                        the task as healthy based on healthcheck parameters. Note that if a change stream request                                  
                                                                        is received during this interval, the delay will be canceled and the takeover will happen                                  
                                                                        immediately, since the incoming request indicates that the task is registered as a target.                                 
                                                                                                                                                                                                   
     --change-streamer-back-pressure-limit-heap-proportion number       default: 0.04                                                                                                              
       ZERO_CHANGE_STREAMER_BACK_PRESSURE_LIMIT_HEAP_PROPORTION env                                                                                                                                
                                                                        The percentage of --max-old-space-size to use as a buffer for absorbing replication                                        
                                                                        stream spikes. When the estimated amount of queued data exceeds this threshold, back pressure                              
                                                                        is applied to the replication stream, delaying downstream sync as a result.                                                
                                                                                                                                                                                                   
                                                                        The threshold was determined empirically with load testing. Higher thresholds have resulted                                
                                                                        in OOMs. Note also that the byte-counting logic in the queue is strictly an underestimate of                               
                                                                        actual memory usage (but importantly, proportionally correct), so the queue is actually                                    
                                                                        using more than what this proportion suggests.                                                                             
                                                                                                                                                                                                   
                                                                        This parameter is exported as an emergency knob to reduce the size of the buffer in the                                    
                                                                        event that the server OOMs from back pressure. Resist the urge to increase this                                            
                                                                        proportion, as it is mainly useful for absorbing periodic spikes and does not meaningfully                                 
                                                                        affect steady-state replication throughput; the latter is determined by other factors such                                 
                                                                        as object serialization and PG throughput                                                                                  
                                                                                                                                                                                                   
                                                                        In other words, the back pressure limit does not constrain replication throughput;                                         
                                                                        rather, it protects the system when the upstream throughput exceeds the downstream                                         
                                                                        throughput.                                                                                                                
                                                                                                                                                                                                   
     --change-streamer-flow-control-consensus-padding-seconds number    default: 1                                                                                                                 
       ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS env                                                                                                                             
                                                                        During periodic flow control checks (every 64kb), the amount of time to wait after the                                     
                                                                        majority of subscribers have acked, after which replication will continue even if                                          
                                                                        some subscribers have yet to ack. (Note that this is not a timeout for the entire send,                                    
                                                                        but a timeout that starts after the majority of receivers have acked.)                                                     
                                                                                                                                                                                                   
                                                                        This allows a bounded amount of time for backlogged subscribers to catch up on each flush                                  
                                                                        without forcing all subscribers to wait for the entire backlog to be processed. It is also                                 
                                                                        useful for mitigating the effect of unresponsive subscribers due to severed websocket                                      
                                                                        connections (until liveness checks disconnect them).                                                                       
                                                                                                                                                                                                   
                                                                        Set this to a negative number to disable early flow control releases. (Not recommended, but                                
                                                                        available as an emergency measure.)                                                                                        
                                                                                                                                                                                                   
     --task-id string                                                   optional                                                                                                                   
       ZERO_TASK_ID env                                                                                                                                                                            
                                                                        Globally unique identifier for the zero-cache instance.                                                                    
                                                                                                                                                                                                   
                                                                        Setting this to a platform specific task identifier can be useful for debugging.                                           
                                                                        If unspecified, zero-cache will attempt to extract the TaskARN if run from within                                          
                                                                        an AWS ECS container, and otherwise use a random string.                                                                   
                                                                                                                                                                                                   
     --per-user-mutation-limit-max number                               optional                                                                                                                   
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                                                                                        
                                                                        The maximum mutations per user within the specified windowMs.                                                              
                                                                        If unset, no rate limiting is enforced.                                                                                    
                                                                                                                                                                                                   
     --per-user-mutation-limit-window-ms number                         default: 60000                                                                                                             
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                                                                                  
                                                                        The sliding window over which the perUserMutationLimitMax is enforced.                                                     
                                                                                                                                                                                                   
     --num-sync-workers number                                          optional                                                                                                                   
       ZERO_NUM_SYNC_WORKERS env                                                                                                                                                                   
                                                                        The number of processes to use for view syncing.                                                                           
                                                                        Leave this unset to use the maximum available parallelism.                                                                 
                                                                        If set to 0, the server runs without sync workers, which is the                                                            
                                                                        configuration for running the replication-manager.                                                                         
                                                                                                                                                                                                   
     --auto-reset boolean                                               default: true                                                                                                              
       ZERO_AUTO_RESET env                                                                                                                                                                         
                                                                        Automatically wipe and resync the replica when replication is halted.                                                      
                                                                        This situation can occur for configurations in which the upstream database                                                 
                                                                        provider prohibits event trigger creation, preventing the zero-cache from                                                  
                                                                        being able to correctly replicate schema changes. For such configurations,                                                 
                                                                        an upstream schema change will instead result in halting replication with an                                               
                                                                        error indicating that the replica needs to be reset.                                                                       
                                                                                                                                                                                                   
                                                                        When auto-reset is enabled, zero-cache will respond to such situations                                                     
                                                                        by shutting down, and when restarted, resetting the replica and all synced                                                 
                                                                        clients. This is a heavy-weight operation and can result in user-visible                                                   
                                                                        slowness or downtime if compute resources are scarce.                                                                      
                                                                                                                                                                                                   
     --replication-lag-report-interval-ms number                        default: 30000                                                                                                             
       ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS env                                                                                                                                                 
                                                                        The minimum interval at which replication lag reports are written upstream and                                             
                                                                        reported via the zero.replication.total_lag opentelemetry metric. Because                                                  
                                                                        replication lag reports are only issued after the previous one was received, the                                           
                                                                        actual interval between reports may be longer when there is a backlog in the                                               
                                                                        replication stream. A negative or 0 value disables lag reporting.                                                          
                                                                                                                                                                                                   
                                                                        This monitoring feature is only support on the postgres upstream type.                                                     
                                                                                                                                                                                                   
     --admin-password string                                            optional                                                                                                                   
       ZERO_ADMIN_PASSWORD env                                                                                                                                                                     
                                                                        A password used to administer zero-cache server, for example to access the                                                 
                                                                        /statz endpoint.                                                                                                           
                                                                                                                                                                                                   
                                                                        A password is optional in development mode but required in production mode.                                                
                                                                                                                                                                                                   
     --websocket-compression boolean                                    default: false                                                                                                             
       ZERO_WEBSOCKET_COMPRESSION env                                                                                                                                                              
                                                                        Enable WebSocket per-message deflate compression.                                                                          
                                                                                                                                                                                                   
                                                                        Compression can reduce bandwidth usage for sync traffic but                                                                
                                                                        increases CPU usage on both client and server. Disabled by default.                                                        
                                                                                                                                                                                                   
                                                                        See: https://github.com/websockets/ws#websocket-compression                                                                
                                                                                                                                                                                                   
     --websocket-compression-options string                             optional                                                                                                                   
       ZERO_WEBSOCKET_COMPRESSION_OPTIONS env                                                                                                                                                      
                                                                        JSON string containing WebSocket compression options.                                                                      
                                                                                                                                                                                                   
                                                                        Only used if websocketCompression is enabled.                                                                              
                                                                                                                                                                                                   
                                                                        Example: {"zlibDeflateOptions":{"level":3},"threshold":1024}                                                               
                                                                                                                                                                                                   
                                                                        See https://github.com/websockets/ws/blob/master/doc/ws.md#new-websocketserveroptions-callback for available options.      
                                                                                                                                                                                                   
     --websocket-max-payload-bytes number                               default: 10485760                                                                                                          
       ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES env                                                                                                                                                        
                                                                        Maximum size of incoming WebSocket messages in bytes.                                                                      
                                                                                                                                                                                                   
                                                                        Messages exceeding this limit are rejected before parsing.                                                                 
                                                                        Default: 10MB (10 * 1024 * 1024 = 10485760)                                                                                
                                                                                                                                                                                                   
     --litestream-executable string                                     optional                                                                                                                   
       ZERO_LITESTREAM_EXECUTABLE env                                                                                                                                                              
                                                                        Path to the litestream executable. This must be built from the                                                             
                                                                        rocicorp/litestream fork. Support for the official binary at v0.5.x                                                        
                                                                        is planned.                                                                                                                
                                                                                                                                                                                                   
     --litestream-executable-v5 string                                  optional                                                                                                                   
       ZERO_LITESTREAM_EXECUTABLE_V5 env                                                                                                                                                           
                                                                        The v0.5.x litestream executable which is used for restoring the backup                                                    
                                                                        backup when ZERO_LITESTREAM_RESTORE_USING_V5 is specified.                                                                 
                                                                        litestream v0.5.8+ can restore from both v0.3.x and v0.5.x backup formats,                                                 
                                                                        affording forwards compatibility with a future zero-cache                                                                  
                                                                        version that will use litestream v0.5.x to backup the replica.                                                             
                                                                                                                                                                                                   
     --litestream-restore-using-v5 boolean                              default: false                                                                                                             
       ZERO_LITESTREAM_RESTORE_USING_V5 env                                                                                                                                                        
                                                                        Restores the backup using the ZERO_LITESTREAM_EXECUTABLE_V5 if specified.                                                  
                                                                                                                                                                                                   
     --litestream-config-path string                                    default: "./src/services/litestream/config.yml"                                                                            
       ZERO_LITESTREAM_CONFIG_PATH env                                                                                                                                                             
                                                                        Path to the litestream yaml config file. zero-cache will run this with its                                                 
                                                                        environment variables, which can be referenced in the file via \${ENV}                                                      
                                                                        substitution, for example:                                                                                                 
                                                                        * ZERO_REPLICA_FILE for the db path                                                                                        
                                                                        * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                                                   
                                                                        * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                                              
                                                                        * ZERO_LOG_FORMAT for the log type                                                                                         
                                                                                                                                                                                                   
     --litestream-log-level debug,info,warn,error                       default: "warn"                                                                                                            
       ZERO_LITESTREAM_LOG_LEVEL env                                                                                                                                                               
                                                                                                                                                                                                   
     --litestream-backup-url string                                     optional                                                                                                                   
       ZERO_LITESTREAM_BACKUP_URL env                                                                                                                                                              
                                                                        The location of the litestream backup, usually an s3:// URL.                                                               
                                                                        This is only consulted by the replication-manager.                                                                         
                                                                        view-syncers receive this information from the replication-manager.                                                        
                                                                                                                                                                                                   
     --litestream-endpoint string                                       optional                                                                                                                   
       ZERO_LITESTREAM_ENDPOINT env                                                                                                                                                                
                                                                        The S3-compatible endpoint URL to use for the litestream backup. Only required for non-AWS services.                       
                                                                        The replication-manager and view-syncers must have the same endpoint.                                                      
                                                                                                                                                                                                   
     --litestream-region string                                         optional                                                                                                                   
       ZERO_LITESTREAM_REGION env                                                                                                                                                                  
                                                                        The AWS region for the litestream backup bucket. Required for non-standard AWS partitions                                  
                                                                        (e.g. GovCloud us-gov-west-1) where Litestream cannot auto-detect the region.                                              
                                                                        The replication-manager and view-syncers must have the same region.                                                        
                                                                                                                                                                                                   
     --litestream-port number                                           optional                                                                                                                   
       ZERO_LITESTREAM_PORT env                                                                                                                                                                    
                                                                        Port on which litestream exports metrics, used to determine the replication                                                
                                                                        watermark up to which it is safe to purge change log records.                                                              
                                                                                                                                                                                                   
                                                                        If unspecified, defaults to --port + 2.                                                                                    
                                                                                                                                                                                                   
     --litestream-checkpoint-threshold-mb number                        default: 40                                                                                                                
       ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB env                                                                                                                                                 
                                                                        The size of the WAL file at which to perform an SQlite checkpoint to apply                                                 
                                                                        the writes in the WAL to the main database file. Each checkpoint creates                                                   
                                                                        a new WAL segment file that will be backed up by litestream. Smaller thresholds                                            
                                                                        may improve read performance, at the expense of creating more files to download                                            
                                                                        when restoring the replica from the backup.                                                                                
                                                                                                                                                                                                   
     --litestream-min-checkpoint-page-count number                      optional                                                                                                                   
       ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT env                                                                                                                                               
                                                                        The WAL page count at which SQLite attempts a PASSIVE checkpoint, which                                                    
                                                                        transfers pages to the main database file without blocking writers.                                                        
                                                                        Defaults to checkpointThresholdMB * 250 (since SQLite page size is 4KB).                                                   
                                                                                                                                                                                                   
     --litestream-max-checkpoint-page-count number                      optional                                                                                                                   
       ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT env                                                                                                                                               
                                                                        The WAL page count at which SQLite performs a RESTART checkpoint, which                                                    
                                                                        blocks writers until complete. Defaults to minCheckpointPageCount * 10.                                                    
                                                                        Set to 0 to disable RESTART checkpoints entirely.                                                                          
                                                                                                                                                                                                   
     --litestream-incremental-backup-interval-minutes number            default: 15                                                                                                                
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES env                                                                                                                                     
                                                                        The interval between incremental backups of the replica. Shorter intervals                                                 
                                                                        reduce the amount of change history that needs to be replayed when catching                                                
                                                                        up a new view-syncer, at the expense of increasing the number of files needed                                              
                                                                        to download for the initial litestream restore.                                                                            
                                                                                                                                                                                                   
     --litestream-snapshot-backup-interval-hours number                 default: 12                                                                                                                
       ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS env                                                                                                                                          
                                                                        The interval between snapshot backups of the replica. Snapshot backups                                                     
                                                                        make a full copy of the database to a new litestream generation. This                                                      
                                                                        improves restore time at the expense of bandwidth. Applications with a                                                     
                                                                        large database and low write rate can increase this interval to reduce                                                     
                                                                        network usage for backups (litestream defaults to 24 hours).                                                               
                                                                                                                                                                                                   
     --litestream-restore-parallelism number                            default: 48                                                                                                                
       ZERO_LITESTREAM_RESTORE_PARALLELISM env                                                                                                                                                     
                                                                        The number of WAL files to download in parallel when performing the                                                        
                                                                        initial restore of the replica from the backup.                                                                            
                                                                                                                                                                                                   
     --litestream-multipart-concurrency number                          default: 48                                                                                                                
       ZERO_LITESTREAM_MULTIPART_CONCURRENCY env                                                                                                                                                   
                                                                        The number of parts (of size --litestream-multipart-size bytes)                                                            
                                                                        to upload or download in parallel when backing up or restoring the snapshot.                                               
                                                                                                                                                                                                   
     --litestream-multipart-size number                                 default: 16777216                                                                                                          
       ZERO_LITESTREAM_MULTIPART_SIZE env                                                                                                                                                          
                                                                        The size of each part when uploading or downloading the snapshot with                                                      
                                                                        --multipart-concurrency. Note that up to concurrency * size                                                                
                                                                        bytes of memory are used when backing up or restoring the snapshot.                                                        
                                                                                                                                                                                                   
     --storage-db-tmp-dir string                                        optional                                                                                                                   
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                                                                                 
                                                                        tmp directory for IVM operator storage. Leave unset to use os.tmpdir()                                                     
                                                                                                                                                                                                   
     --initial-sync-table-copy-workers number                           default: 5                                                                                                                 
       ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS env                                                                                                                                                    
                                                                        The number of parallel workers used to copy tables during initial sync.                                                    
                                                                        Each worker uses a database connection and will buffer up to (approximately)                                               
                                                                        10 MB of table data in memory during initial sync. Increasing the number of                                                
                                                                        workers may improve initial sync speed; however, note that local disk throughput                                           
                                                                        (i.e. IOPS), upstream CPU, and network bandwidth may also be bottlenecks.                                                  
                                                                                                                                                                                                   
     --initial-sync-text-copy boolean                                   default: false                                                                                                             
       ZERO_INITIAL_SYNC_TEXT_COPY env                                                                                                                                                             
                                                                        Use text-format COPY instead of binary COPY for initial sync and                                                           
                                                                        backfill streaming. This is slower but can work around issues with                                                         
                                                                        binary encoding of certain data types.                                                                                     
                                                                                                                                                                                                   
     --shadow-sync-enabled boolean                                      default: false                                                                                                             
       ZERO_SHADOW_SYNC_ENABLED env                                                                                                                                                                
                                                                        Periodically exercises the initial-sync code path against a sample of                                                      
                                                                        rows from every published table, writing to a throwaway SQLite database.                                                   
                                                                        This acts as a canary: if the real initial-sync path ever breaks (schema                                                   
                                                                        drift, PG version quirks, etc.), the shadow run fails before a customer                                                    
                                                                        actually needs a full reset.                                                                                               
                                                                                                                                                                                                   
     --shadow-sync-interval-hours number                                default: 12                                                                                                                
       ZERO_SHADOW_SYNC_INTERVAL_HOURS env                                                                                                                                                         
                                                                        The interval between shadow initial-sync runs, in hours. The first                                                         
                                                                        run fires within [2/3, 1) of this interval after startup, so the                                                           
                                                                        canary completes at least once per task lifetime (the replication                                                          
                                                                        manager is restarted every ~24h) while still jittering so a fleet                                                          
                                                                        restart does not cause all tasks to canary simultaneously.                                                                 
                                                                                                                                                                                                   
     --shadow-sync-sample-rate number                                   default: 0.1                                                                                                               
       ZERO_SHADOW_SYNC_SAMPLE_RATE env                                                                                                                                                            
                                                                        The BERNOULLI sampling rate for each table (0 < rate <= 1). A value of                                                     
                                                                        1 disables sampling and copies all rows (still subject to                                                                  
                                                                        max-rows-per-table).                                                                                                       
                                                                                                                                                                                                   
     --shadow-sync-max-rows-per-table number                            default: 10000                                                                                                             
       ZERO_SHADOW_SYNC_MAX_ROWS_PER_TABLE env                                                                                                                                                     
                                                                        The hard upper bound on rows copied per table per shadow run. Guards                                                       
                                                                        against unexpectedly large tables consuming disk / upstream bandwidth.                                                     
                                                                                                                                                                                                   
     --lazy-startup boolean                                             default: false                                                                                                             
       ZERO_LAZY_STARTUP env                                                                                                                                                                       
                                                                        Delay starting the majority of zero-cache until first request.                                                             
                                                                                                                                                                                                   
                                                                        This is mainly intended to avoid connecting to Postgres replication stream                                                 
                                                                        until the first request is received, which can be useful i.e., for preview instances.                                      
                                                                                                                                                                                                   
                                                                        Currently only supported in single-node mode.                                                                              
                                                                                                                                                                                                   
     --server-version string                                            optional                                                                                                                   
       ZERO_SERVER_VERSION env                                                                                                                                                                     
                                                                        The version string outputted to logs when the server starts up.                                                            
                                                                                                                                                                                                   
     --enable-telemetry boolean                                         default: true                                                                                                              
       ZERO_ENABLE_TELEMETRY env                                                                                                                                                                   
                                                                        Set to false to opt out of telemetry collection.                                                                           
                                                                                                                                                                                                   
                                                                        This helps us improve Zero by collecting anonymous usage data.                                                             
                                                                        Setting the DO_NOT_TRACK environment variable also disables telemetry.                                                     
                                                                                                                                                                                                   
     --cloud-event-sink-env string                                      optional                                                                                                                   
       ZERO_CLOUD_EVENT_SINK_ENV env                                                                                                                                                               
                                                                        ENV variable containing a URI to a CloudEvents sink. When set, ZeroEvents                                                  
                                                                        will be published to the sink as the data field of CloudEvents.                                                            
                                                                        The source field of the CloudEvents will be set to the ZERO_TASK_ID,                                                       
                                                                        along with any extension attributes specified by the ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV.                             
                                                                                                                                                                                                   
                                                                        This configuration is modeled to easily integrate with a knative K_SINK binding,                                           
                                                                        (i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).                                     
                                                                        However, any CloudEvents sink can be used.                                                                                 
                                                                                                                                                                                                   
     --cloud-event-extension-overrides-env string                       optional                                                                                                                   
       ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV env                                                                                                                                                
                                                                        ENV variable containing a JSON stringified object with an extensions field                                                 
                                                                        containing attributes that should be added or overridden on outbound CloudEvents.                                          
                                                                                                                                                                                                   
                                                                        This configuration is modeled to easily integrate with a knative K_CE_OVERRIDES binding,                                   
                                                                        (i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).                                     
                                                                                                                                                                                                   
     --go-sidecar-enabled boolean                                       default: true                                                                                                              
       ZERO_GO_SIDECAR_ENABLED env                                                                                                                                                                 
                                                                        Offload IVM compute (advance + hydrate) to the companion Go sidecar process                                                
                                                                        instead of running the TypeScript operator tree inline. The Go path runs queries                                           
                                                                        for each client group in parallel and is the production-ready primary path                                                 
                                                                        (see go-ivm/REVIEW-final.md).                                                                                              
                                                                                                                                                                                                   
                                                                        When false, all IVM compute runs in the TS path. When true, requires the                                                   
                                                                        goSidecar.binaryPath binary to be reachable; if the sidecar fails to start                                                 
                                                                        or returns an error, the PipelineDriver falls back to the TS path.                                                         
                                                                                                                                                                                                   
     --go-sidecar-shadow-mode boolean                                   default: false                                                                                                             
       ZERO_GO_SIDECAR_SHADOW_MODE env                                                                                                                                                             
                                                                        Run BOTH the Go and TS IVM paths and compare results. TS remains the source                                                
                                                                        of truth; Go results are discarded after comparison. Mismatches are logged                                                 
                                                                        at error level. Used to validate the sidecar before switching to Go-primary.                                               
                                                                                                                                                                                                   
                                                                        Requires goSidecar.enabled to also be true.                                                                                
                                                                                                                                                                                                   
     --go-sidecar-shadow-verbose boolean                                default: false                                                                                                             
       ZERO_GO_SIDECAR_SHADOW_VERBOSE env                                                                                                                                                          
                                                                        In shadow mode, include full row contents in mismatch logs. Default is to                                                  
                                                                        redact rows to (type, queryID, rowKey) for PII safety. Only enable in                                                      
                                                                        development or with explicit log-PII review.                                                                               
                                                                                                                                                                                                   
     --go-sidecar-binary-path string                                    default: "go-ivm-sidecar"                                                                                                  
       ZERO_GO_SIDECAR_BINARY_PATH env                                                                                                                                                             
                                                                        Path to the compiled go-ivm-sidecar binary. The default resolves via PATH;                                                 
                                                                        override with an absolute path when the binary is not on PATH (e.g. when                                                   
                                                                        running zero-cache from a non-standard install location).                                                                  
                                                                                                                                                                                                   
     --go-sidecar-transport socket,napi                                 default: "napi"                                                                                                            
       ZERO_GO_SIDECAR_TRANSPORT env                                                                                                                                                               
                                                                        How the worker talks to the Go IVM engine.                                                                                 
                                                                                                                                                                                                   
                                                                        socket: spawn (or connect to) a separate sidecar process                                                                   
                                                                        over a Unix domain socket with length-prefixed msgpack frames.                                                             
                                                                                                                                                                                                   
                                                                        napi (default — the production path): load the Go engine IN-PROCESS via the goivm_napi N-API addon +                       
                                                                        libgoivm c-shared library (goSidecar.napiLibPath). No child process, no                                                    
                                                                        socket, no per-frame syscalls; results can be delivered row-by-row                                                         
                                                                        (goSidecar.napiRowMode). Requires the out-of-band build artifacts                                                          
                                                                        (addon .node + libgoivm); if either is missing, startup fails and the                                                      
                                                                        worker falls back to the TS path. Incompatible with                                                                        
                                                                        goSidecar.externallyManaged (an in-process engine cannot be shared                                                         
                                                                        across workers) — if both are set, externallyManaged wins and transport                                                    
                                                                        reverts to socket. NOTE: a Go runtime cannot be unloaded or restarted                                                      
                                                                        in-process, so "sidecar restart" recovery does not exist in this mode.                                                     
                                                                        A fatal Go error takes down the whole syncer worker, and a POST-START                                                      
                                                                        transport failure deliberately crashes the worker too — once client                                                        
                                                                        groups are Go-owned, "fall back to TS" is unsound (lean-primary keeps                                                      
                                                                        only stub pipelines); the supervisor restart restores a working state.                                                     
                                                                                                                                                                                                   
     --go-sidecar-napi-lib-path string                                  default: "libgoivm.so"                                                                                                     
       ZERO_GO_SIDECAR_NAPI_LIB_PATH env                                                                                                                                                           
                                                                        Path to the libgoivm c-shared library for goSidecar.transport=napi                                                         
                                                                        (ignored otherwise). The default bare name resolves via the platform's                                                     
                                                                        dynamic-linker search path (LD_LIBRARY_PATH etc.); use an absolute path                                                    
                                                                        in containers (e.g. /usr/local/lib/libgoivm.so). Built in the go-ivm                                                       
                                                                        repo with: go build -tags "libsqlite3 napilib" -buildmode=c-shared.                                                        
                                                                                                                                                                                                   
     --go-sidecar-napi-row-mode boolean                                 default: true                                                                                                              
       ZERO_GO_SIDECAR_NAPI_ROW_MODE env                                                                                                                                                           
                                                                        When the NAPI transport is active, deliver hydrate/advance results from                                                    
                                                                        the Go engine ROW BY ROW (compact binary records over the in-process                                                       
                                                                        queue) instead of accumulating them into msgpack chunk frames. This is                                                     
                                                                        the point of the NAPI transport — each row is visible to TS as soon as                                                     
                                                                        Go produces it. Disable only for A/B measurement (napi-frames vs                                                           
                                                                        napi-rows); has no effect on the socket transport.                                                                         
                                                                                                                                                                                                   
     --go-sidecar-pull-hydrate boolean                                  default: true                                                                                                              
       ZERO_GO_SIDECAR_PULL_HYDRATE env                                                                                                                                                            
                                                                        PULL-based hydration over the NAPI transport (ABI v3): Go produces                                                         
                                                                        hydrate rows only as the view-syncer consumes them — consumer demand                                                       
                                                                        crosses the boundary as credit grants, and abandoning a hydrate                                                            
                                                                        mid-stream cancels the Go producer (cursor close, reader release).                                                         
                                                                        This is the TS-faithful demand discipline: one next() ⇒ one row,                                                           
                                                                        bounded lookahead of goSidecar.pullWindow.                                                                                 
                                                                                                                                                                                                   
                                                                        Requires goSidecar.transport=napi + goSidecar.napiRowMode                                                                  
                                                                        and a v3 libgoivm; silently degrades to push delivery otherwise.                                                           
                                                                        On by default (the production path); disable per deployment to fall                                                        
                                                                        back to push delivery.                                                                                                     
                                                                                                                                                                                                   
     --go-sidecar-pull-window number                                    default: 64                                                                                                                
       ZERO_GO_SIDECAR_PULL_WINDOW env                                                                                                                                                             
                                                                        Pull-hydration lookahead window W (goSidecar.pullHydrate): Go may                                                          
                                                                        run at most W row deliveries ahead of what the view-syncer consumed;                                                       
                                                                        credits top back up to W at the W/2 low-water mark. 1 = strict lockstep                                                    
                                                                        (literal TS generator semantics, one next() per row — the parity-test                                                      
                                                                        pin); 64 (default) keeps identical semantics with 32x fewer park/wake                                                      
                                                                        cycles and boundary calls on the hot path.                                                                                 
                                                                                                                                                                                                   
     --go-sidecar-externally-managed boolean                            default: false                                                                                                             
       ZERO_GO_SIDECAR_EXTERNALLY_MANAGED env                                                                                                                                                      
                                                                        When true, the Go sidecar process is owned by something outside zero-cache                                                 
                                                                        (e.g., a container entrypoint that spawns one shared sidecar across all                                                    
                                                                        worker processes). The worker's SidecarManager skips spawning and binary                                                   
                                                                        discovery — it just connects to the socket at goSidecar.socketPath                                                         
                                                                        and reconnects via a health-check ticker if the connection drops.                                                          
                                                                                                                                                                                                   
                                                                        Use this to consolidate from one-sidecar-per-worker to one-sidecar-per-cache.                                              
                                                                        With per-worker sidecars, inter-cg parallelism is capped at                                                                
                                                                        active_cgs / num_sync_workers; with a shared sidecar it scales                                                             
                                                                        with active_cgs, so increasing worker count doesn't fragment cg                                                            
                                                                        co-location and shrink per-sidecar batches.                                                                                
                                                                                                                                                                                                   
     --go-sidecar-socket-path string                                    optional                                                                                                                   
       ZERO_GO_SIDECAR_SOCKET_PATH env                                                                                                                                                             
                                                                        Unix socket path the sidecar listens on. Required when                                                                     
                                                                        goSidecar.externallyManaged is true (otherwise defaults to a                                                               
                                                                        per-worker /tmp/go-ivm-<pid>.sock). Both the sidecar owner and the                                                         
                                                                        connecting workers must agree on this path.                                                                                
                                                                                                                                                                                                   
     --go-sidecar-drift-audit-interval-ms number                        default: 60000                                                                                                             
       ZERO_GO_SIDECAR_DRIFT_AUDIT_INTERVAL_MS env                                                                                                                                                 
                                                                        In Go-primary mode (enabled=true, shadowMode=false), how often to run a                                                    
                                                                        sampled-shadow drift audit. Each interval, the PipelineDriver picks one                                                    
                                                                        random active query and re-hydrates it on BOTH TS and Go from the current                                                  
                                                                        snapshot, comparing the results to catch silent drift. Mismatches are                                                      
                                                                        logged at error level and counted via the ivm.drift-audit-mismatch                                                         
                                                                        metric. Has no effect in shadow mode (which audits every query) or when                                                    
                                                                        Go is disabled.                                                                                                            
                                                                                                                                                                                                   
                                                                        Set to 0 to disable. Default 60000ms (one query per minute per group).                                                     
                                                                        Lower values give faster drift detection but more sidecar load.                                                            
                                                                                                                                                                                                   
     --go-sidecar-drift-audit-sql-ground-truth boolean                  default: true                                                                                                              
       ZERO_GO_SIDECAR_DRIFT_AUDIT_SQL_GROUND_TRUTH env                                                                                                                                            
                                                                        When the drift audit fires, also run a raw SQL query on the snapshot's                                                     
                                                                        SQLite replica as a third opinion and compare Go's output against SQL                                                      
                                                                        truth (in addition to the TS-audit comparison). Catches Go-vs-SQL set                                                      
                                                                        and content drift directly — without this, only Go-vs-TS-audit divergence                                                  
                                                                        is surfaced, which can be noisy (TS-audit has known boundary-drop bugs).                                                   
                                                                                                                                                                                                   
                                                                        Disable to skip the per-audit SQL re-query if it shows up in replica                                                       
                                                                        load. When off, the audit falls back to the legacy TS-vs-Go set                                                            
                                                                        comparison only.                                                                                                           
                                                                                                                                                                                                   
     --go-sidecar-divergence-capture-dir string                         optional                                                                                                                   
       ZERO_GO_SIDECAR_DIVERGENCE_CAPTURE_DIR env                                                                                                                                                  
                                                                        When set, the shadow comparator captures a reproducible snapshot of                                                        
                                                                        each divergence it can't adjudicate (ts-only / oracle-blind /                                                              
                                                                        go-vs-sql-drift / raw MISMATCH) to this directory: a VACUUM INTO copy                                                      
                                                                        of the SQLite replica at that stateVersion plus a JSON file with the                                                       
                                                                        AST, queryID, operation, both sides' changes, and the SQL verdict.                                                         
                                                                        Captures are rate-capped (one per queryID per minute) and gated off                                                        
                                                                        by default (empty/unset = no capture) because VACUUM INTO copies the                                                       
                                                                        whole DB on the divergence hot path. Intended for offline replay via                                                       
                                                                        the go-ivm testharness RunTestCaseFromSnapshot loader. NEVER point                                                         
                                                                        this at a directory inside the public go-ivm repo — real prod rows                                                         
                                                                        have PII; sanitize captures before committing any repro.                                                                   
                                                                                                                                                                                                   
     --go-sidecar-advance-drive boolean                                 default: true                                                                                                              
       ZERO_GO_SIDECAR_ADVANCE_DRIVE env                                                                                                                                                           
                                                                        Snapshotter-in-Go (P2): DRIVE Go's engine from its own derived diff.                                                       
                                                                        In shadow mode the Go advance is sourced via the sidecar's advanceToHead                                                   
                                                                        RPC (frame-coordinated against the Snapshotter's pinned frame — no                                                         
                                                                        TS-shipped SnapshotChange[]) and the resulting RowChanges are compared                                                     
                                                                        against TS's. This validates the self-consistent Go-primary advance path                                                   
                                                                        end-to-end before CVR version authority moves to Go.                                                                       
                                                                                                                                                                                                   
                                                                        Requires goSidecar.shadowMode and a sidecar launched with                                                                  
                                                                        GO_IVM_ADVANCE_DRIVE=true (which implies advanceToHead) +                                                                  
                                                                        GO_IVM_SOURCE_MODE=table. Implies goSidecar.advanceToHead.                                                                 
                                                                                                                                                                                                   
     --go-sidecar-advance-to-head boolean                               default: false                                                                                                             
       ZERO_GO_SIDECAR_ADVANCE_TO_HEAD env                                                                                                                                                         
                                                                        Snapshotter-in-Go (P1): on each shadow advance, ALSO call the sidecar's                                                    
                                                                        advanceToHead RPC so the Go side derives its OWN snapshot diff from the                                                    
                                                                        replica's changeLog2, then compare it against the diff TS computed for the                                                 
                                                                        same advance. This validates the ported Go Snapshotter's fidelity before                                                   
                                                                        Go-derived diffs feed the CVR (P2).                                                                                        
                                                                                                                                                                                                   
                                                                        Requires goSidecar.shadowMode (the comparison runs in the shadow                                                           
                                                                        advance path), a protocolRev-9 sidecar (the connect handshake enforces                                                     
                                                                        exact protocol equality) launched with                                                                                     
                                                                        GO_IVM_ADVANCE_TO_HEAD=true and GO_IVM_SOURCE_MODE=table.                                                                  
                                                                        Mismatches are logged at error level under the [go-diff-shadow] tag.                                                       
                                                                                                                                                                                                   
     --go-sidecar-go-primary-trigger boolean                            default: true                                                                                                              
       ZERO_GO_SIDECAR_GO_PRIMARY_TRIGGER env                                                                                                                                                      
                                                                        Snapshotter-in-Go (P2c): in Go-PRIMARY mode (enabled=true,                                                                 
                                                                        shadowMode=false), source the user-query advance via the sidecar's                                                         
                                                                        advanceToHead RPC (Go derives its OWN diff + drives its OWN engine,                                                        
                                                                        frame-coordinated) instead of shipping it the TS-derived diff via                                                          
                                                                        advanceStream. This makes Go self-consistent (no frame-timing drift) and                                                   
                                                                        moves user-query CVR version authority to Go: the CVR stateVersion is                                                      
                                                                        stamped at min(V_ts, V_go) — the completeness floor both the                                                               
                                                                        TS-internal and Go-user pipelines have crossed (see                                                                        
                                                                        go-ivm/DESIGN-snapshotter-port.md §10).                                                                                    
                                                                                                                                                                                                   
                                                                        Has no effect in shadow mode. Requires a sidecar launched with                                                             
                                                                        GO_IVM_ADVANCE_TO_HEAD=true + GO_IVM_ADVANCE_DRIVE=true +                                                                  
                                                                        GO_IVM_SOURCE_MODE=table. When off, Go-primary keeps the push                                                              
                                                                        (advanceStream) path and the CVR stamps at TS's version.                                                                   
                                                                                                                                                                                                   
     --go-sidecar-lean-primary boolean                                  default: true                                                                                                              
       ZERO_GO_SIDECAR_LEAN_PRIMARY env                                                                                                                                                            
                                                                        Snapshotter-in-Go (P3): in Go-PRIMARY mode (enabled=true,                                                                  
                                                                        shadowMode=false), stop TS from walking USER-table changes during                                                          
                                                                        advance. TS already holds only no-op STUB pipelines for user queries                                                       
                                                                        (Go owns them) and keeps its user TableSources current via the snapshot                                                    
                                                                        setDB on every advance — NOT via these pushes — so the walk is pure                                                        
                                                                        redundant work. With this on, TS's #advance processes only                                                                 
                                                                        internal/control-plane changes (lmids, mutationResults); user-table                                                        
                                                                        changes are dropped from TS's replay (and, in trigger mode, never                                                          
                                                                        materialized at all). The drift audit and cold fallback still                                                              
                                                                        re-hydrate user queries on demand from the snapshot-backed TableSources,                                                   
                                                                        so correctness is preserved.                                                                                               
                                                                                                                                                                                                   
                                                                        Has no effect in shadow mode (TS is authoritative there and must walk                                                      
                                                                        the full diff). Safe to combine with goSidecar.goPrimaryTrigger.                                                           
                                                                                                                                                                                                   
    "
  `);
});

test.each([['has/slashes'], ['has-dashes'], ['has.dots']])(
  '--app-id %s',
  appID => {
    const logger = {info: vi.fn()};
    expect(() =>
      parseOptionsAdvanced(zeroOptions, {
        argv: ['--app-id', appID],
        envNamePrefix: 'ZERO_',
        allowUnknown: false,
        allowPartial: true,
        env: {},
        logger,
        exit,
      }),
    ).toThrowError(INVALID_APP_ID_MESSAGE);
  },
);

test.each([['isok'], ['has_underscores'], ['1'], ['123']])(
  '--app-id %s',
  appID => {
    const {config} = parseOptionsAdvanced(zeroOptions, {
      argv: ['--app-id', appID],
      envNamePrefix: 'ZERO_',
      allowUnknown: false,
      allowPartial: true,
    });
    expect(config.app.id).toBe(appID);
  },
);

test('--shard-id disallowed', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptionsAdvanced(zeroOptions, {
      argv: ['--shard-id', 'prod'],
      envNamePrefix: 'ZERO_',
      allowUnknown: false,
      allowPartial: true,
      env: {},
      logger,
      exit,
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: ZERO_SHARD_ID is no longer an option. Please use ZERO_APP_ID instead.]`,
  );
});
