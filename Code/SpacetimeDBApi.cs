// SpacetimeDB.cs
// Rewritten based on confirmed-working s&box patterns from the legacy 1.0 API.
//
// Key architecture decisions (all validated against working code):
//   - Reducer calls go via HTTP POST  (not WebSocket)
//   - WebSocket is subscribe-only
//   - Identity + WS token are fetched via HTTP before connecting
//   - WebSocket.Send(string) is used directly — no byte encoding needed
//   - Subscribe message is sent immediately upon receiving IdentityToken
//   - OnDisconnected signature is (int code, string reason)
//   - Row parsing uses JsonDocument / JsonElement throughout
//
// USAGE:
//   var db = new SpacetimeDbClient("https://maincloud.spacetimedb.com", "my-database");
//   db.OnConnected    += () => Log.Info("Ready!");
//   db.OnTableInsert  += (table, row) => Log.Info($"{table}: {row.GetRawText()}");
//   db.OnTableDelete  += (table, row) => { };
//   db.OnReducerEvent += (reducer, status) => { };
//   await db.ConnectAsync();                              // fetches identity, opens WS
//   db.Subscribe("player", "chat_message");              // sends SELECT * FROM each
//   await db.CallReducerAsync("set_name", "Hero");       // HTTP POST

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Sandbox;
using Sandbox.Diagnostics;

namespace SpacetimeDB
{
	public sealed class SpacetimeDbClient : IDisposable
	{
		private static readonly Logger Log = new( "SpacetimeDB" );

		// ── Config ────────────────────────────────────────────────────────────

		public string ServerUrl   { get; }   // e.g. "https://maincloud.spacetimedb.com"
		public string ModuleName  { get; }   // database name
		public string Identity    { get; private set; }
		public bool   IsConnected => _ws?.IsConnected ?? false;

		// ── Events ────────────────────────────────────────────────────────────

		/// <summary>Fired after IdentityToken received AND initial subscription applied.</summary>
		public event Action OnConnected;

		/// <summary>Fired when the WebSocket closes.</summary>
		public event Action<int, string> OnDisconnected;

		/// <summary>Fired for every inserted row in any subscribed table.</summary>
		public event Action<string, JsonElement> OnTableInsert;

		/// <summary>Fired for every deleted row in any subscribed table.</summary>
		public event Action<string, JsonElement> OnTableDelete;

		/// <summary>Fired when a reducer transaction update arrives (status = "Committed" | "Failed" | ...).</summary>
		public event Action<string, string> OnReducerEvent;

		/// <summary>Fired on any WebSocket or parse error.</summary>
		public event Action<string> OnError;

		// ── Internals ─────────────────────────────────────────────────────────

		private string   _authToken;
		private string   _wsToken;
		private WebSocket _ws;
		private uint      _reqId = 0;
		private bool      _firedConnected;

		// Queued pending subscriptions sent before IdentityToken arrives.
		private readonly List<string[]> _pendingSubscriptions = new();

		// ── Ctor ──────────────────────────────────────────────────────────────

		public SpacetimeDbClient( string serverUrl, string moduleName )
		{
			ServerUrl  = serverUrl.TrimEnd( '/' );
			ModuleName = moduleName;
		}

		// ── Connection ────────────────────────────────────────────────────────

		/// <summary>
		/// Full connection flow:
		///   1. POST /v1/identity           → get identity + auth token
		///   2. POST /v1/identity/websocket-token → get a short-lived WS token
		///   3. Open WebSocket with ?token=...
		/// OnConnected fires after IdentityToken + first InitialSubscription.
		/// </summary>
		public async Task ConnectAsync()
		{
			Log.Info( $"Connecting to {ServerUrl} / {ModuleName}..." );

			// Step 1 – get identity
			await FetchIdentityAsync();
			Log.Info( $"[DEBUG] authToken length={_authToken?.Length}, starts={_authToken?[..Math.Min(20,_authToken.Length)]}" );
			// Step 2 – get WS token
			await FetchWebSocketTokenAsync();

			// Step 3 – open WebSocket
			OpenWebSocket();
		}

// ── Replace these two methods in SpacetimeDB.cs ───────────────────────────────

		private async Task FetchIdentityAsync()
		{
			var url      = $"{ServerUrl}/v1/identity";
			var response = await Http.RequestStringAsync( url, "POST" );

			using var doc  = JsonDocument.Parse( response );
			var       root = doc.RootElement;

			_authToken = root.GetProperty( "token" ).GetString();
			Identity   = root.GetProperty( "identity" ).GetString();

			Log.Info( $"Identity: {Identity[..Math.Min( 12, Identity.Length )]}..." );
			Log.Info( $"Token length: {_authToken?.Length}" );
		}

		private async Task FetchWebSocketTokenAsync()
		{
			var url     = $"{ServerUrl}/v1/identity/websocket-token";
			var headers = new Dictionary<string, string>
			{
				{ "Authorization", $"Bearer {_authToken}" }
			};

			var response = await Http.RequestStringAsync( url, "POST", null, headers );

			using var doc = JsonDocument.Parse( response );
			_wsToken = doc.RootElement.GetProperty( "token" ).GetString();
		}

		private void OpenWebSocket()
		{
			var wsBase = ServerUrl
				.Replace( "https://", "wss://" )
				.Replace( "http://",  "ws://"  );

			var encodedModule = ModuleName.Replace( "/", "%2F" );
			var url = $"{wsBase}/v1/database/{encodedModule}/subscribe?token={Uri.EscapeDataString( _wsToken )}";

			_ws = new WebSocket();
			_ws.AddSubProtocol( "v1.json.spacetimedb" );

			_ws.OnMessageReceived += HandleMessage;
			_ws.OnDataReceived    += HandleBinaryData;
			_ws.OnDisconnected    += ( code, reason ) =>
			{
				Log.Warning( $"Disconnected: {code} – {reason}" );
				OnDisconnected?.Invoke( code, reason );
			};

			_ = _ws.Connect( url );
			Log.Info( "WebSocket connecting..." );
		}

		// ── Subscriptions ─────────────────────────────────────────────────────

		/// <summary>
		/// Subscribe to one or more table names.
		/// Generates "SELECT * FROM {table}" for each.
		/// Safe to call before ConnectAsync() — queries are queued.
		/// </summary>
		public void Subscribe( params string[] tableNames )
		{
			var queries = new string[tableNames.Length];
			for ( var i = 0; i < tableNames.Length; i++ )
				queries[i] = $"SELECT * FROM {tableNames[i]}";

			SubscribeWithQuery( queries );
		}

		/// <summary>Subscribe with raw SQL query strings.</summary>
		public void SubscribeWithQuery( params string[] queries )
		{
			if ( IsConnected )
				SendSubscribe( queries );
			else
				_pendingSubscriptions.Add( queries );
		}

		private void SendSubscribe( string[] queries )
		{
			_reqId++;

			// Build JSON manually — avoids all JsonNode whitelist issues.
			var sb = new StringBuilder();
			sb.Append( "{\"Subscribe\":{\"request_id\":" );
			sb.Append( _reqId );
			sb.Append( ",\"query_strings\":[" );
			for ( var i = 0; i < queries.Length; i++ )
			{
				if ( i > 0 ) sb.Append( ',' );
				sb.Append( '"' );
				sb.Append( queries[i].Replace( "\\", "\\\\" ).Replace( "\"", "\\\"" ) );
				sb.Append( '"' );
			}
			sb.Append( "]}}" );

			var json = sb.ToString();
			Log.Info( $"Subscribe → {json}" );
			_ws.Send( json );
		}

		// ── Reducer Calls (HTTP) ──────────────────────────────────────────────

		/// <summary>
		/// Invoke a reducer via HTTP POST.
		/// Args are serialised as a JSON array: ["arg1", 42, true, ...].
		/// Pass no args or null for zero-argument reducers.
		/// </summary>
		public async Task CallReducerAsync( string reducerName, params object[] args )
		{
			var url = $"{ServerUrl}/v1/database/{ModuleName}/call/{Uri.EscapeDataString( reducerName )}";

			var headers = new Dictionary<string, string>
			{
				{ "Authorization", $"Bearer {_authToken}" }
				// No Content-Type here — CreateJsonContent sets it internally.
				// Adding it again sends a duplicate header that Cloudflare rejects with 530.
			};

			// CreateJsonContent sets Content-Type: application/json automatically.
			// Use it exactly as the original working code did.
			var argsList = new List<object>( args ?? Array.Empty<object>() );
			var content  = Http.CreateJsonContent( argsList );

			var response = await Http.RequestStringAsync( url, "POST", content, headers );
			Log.Info( $"CallReducer [{reducerName}] OK: {response?[..Math.Min( response?.Length ?? 0, 80 )]}" );
		}
		// ── Keepalive ─────────────────────────────────────────────────────────
		
		private RealTimeSince _lastPing;
		private const float KeepaliveInterval = 20f; // seconds — under the 30s server idle timeout
		
		/// <summary>
		/// Call this every frame (e.g. from a Component's OnUpdate).
		/// Sends a WebSocket ping frame if the keepalive interval has elapsed.
		/// Required because SpacetimeDB MainCloud closes idle connections after ~30s.
		/// </summary>
		public void Tick()
		{
			if ( !IsConnected ) return;
			if ( _lastPing < KeepaliveInterval ) return;

			_lastPing = 0;
			try
			{
				// Send a WebSocket protocol-level ping frame.
				_ws.Send( "{\"Subscribe\":{\"request_id\":0,\"query_strings\":[]}}" );
			}
			catch ( Exception ex )
			{
				Log.Warning( $"Keepalive ping failed: {ex.Message}" );
			}
		}


		// ── WebSocket message handling ────────────────────────────────────────

		private void HandleBinaryData( Span<byte> data )
		{
			try   { HandleMessage( Encoding.UTF8.GetString( data ) ); }
			catch ( Exception ex ) { OnError?.Invoke( $"BinaryDecodeError: {ex.Message}" ); }
		}

		private void HandleMessage( string raw )
		{
			try
			{
				using var doc  = JsonDocument.Parse( raw );
				var       root = doc.RootElement;

				if ( root.ValueKind != JsonValueKind.Object ) return;

				foreach ( var outer in root.EnumerateObject() )
				{
					switch ( outer.Name )
					{
						// ── Server confirmed our identity ─────────────────────
						case "IdentityToken":
						{
							var body = outer.Value;

							// identity field is {"__identity__": "0x..."}, not a plain string.
							if ( body.TryGetProperty( "identity", out var idObj ) )
							{
								if ( idObj.ValueKind == JsonValueKind.Object &&
								     idObj.TryGetProperty( "__identity__", out var idStr ) )
									Identity = idStr.GetString();
								else if ( idObj.ValueKind == JsonValueKind.String )
									Identity = idObj.GetString();
							}

							Log.Info( $"IdentityToken received. Identity: {Identity}" );

							// Flush any subscriptions that were queued before connection.
							foreach ( var q in _pendingSubscriptions )
								SendSubscribe( q );
							_pendingSubscriptions.Clear();

							break;
						}

						// ── Initial rows for our subscription ─────────────────
						case "InitialSubscription":
						{
							var body = outer.Value;
							if ( body.TryGetProperty( "database_update", out var dbUpdate ) )
								DispatchDatabaseUpdate( dbUpdate );

							// Fire OnConnected exactly once.
							if ( !_firedConnected )
							{
								_firedConnected = true;
								OnConnected?.Invoke();
							}
							break;
						}

						// ── Live transaction update ───────────────────────────
						case "TransactionUpdate":
						{
							var body = outer.Value;

							if ( body.TryGetProperty( "status", out var status ) &&
							     status.TryGetProperty( "Committed", out var committed ) )
								DispatchDatabaseUpdate( committed );

							if ( body.TryGetProperty( "reducer_call", out var rc ) )
							{
								var rName     = rc.TryGetProperty( "reducer_name", out var rn ) ? rn.GetString() ?? "" : "";
								var statusKey = "unknown";
								if ( body.TryGetProperty( "status", out var st ) )
									foreach ( var kv in st.EnumerateObject() )
										{ statusKey = kv.Name; break; }

								OnReducerEvent?.Invoke( rName, statusKey );
							}
							break;
						}

						case "SubscribeError":
						{
							var msg = outer.Value.TryGetProperty( "error", out var e )
								? e.GetString() : "unknown";
							Log.Error( $"SubscribeError: {msg}" );
							OnError?.Invoke( $"SubscribeError: {msg}" );
							break;
						}

						default:
							Log.Info( $"Unhandled tag: {outer.Name}" );
							break;
					}
					break; // only one top-level key per envelope
				}
			}
			catch ( Exception ex )
			{
				OnError?.Invoke( $"ParseError: {ex.Message}" );
				Log.Error( $"ParseError: {ex.Message}\n{raw[..Math.Min( raw.Length, 200 )]}" );
			}
		}

		private void DispatchDatabaseUpdate( JsonElement dbUpdate )
		{
			if ( dbUpdate.ValueKind != JsonValueKind.Object ) return;
			if ( !dbUpdate.TryGetProperty( "tables", out var tables ) ) return;
			if ( tables.ValueKind != JsonValueKind.Array ) return;

			foreach ( var tableElem in tables.EnumerateArray() )
			{
				var tableName = tableElem.TryGetProperty( "table_name", out var tn )
					? tn.GetString() ?? "unknown" : "unknown";

				if ( !tableElem.TryGetProperty( "updates", out var updates ) ) continue;

				foreach ( var update in updates.EnumerateArray() )
				{
					DispatchRows( tableName, update, "inserts",  OnTableInsert );
					DispatchRows( tableName, update, "deletes",  OnTableDelete );
				}
			}
		}

		private void DispatchRows( string tableName, JsonElement update, string key,
			Action<string, JsonElement> handler )
		{
			if ( handler == null ) return;
			if ( !update.TryGetProperty( key, out var rows ) ) return;
			Log.Info( $"[STDB] DispatchRows {tableName}/{key}: {rows.GetArrayLength()} rows" );
			if ( rows.ValueKind != JsonValueKind.Array ) return;

			foreach ( var row in rows.EnumerateArray() )
			{
				try
				{
					if ( row.ValueKind == JsonValueKind.String )
					{
						// Row is an escaped JSON string — parse it first.
						using var inner = JsonDocument.Parse( row.GetString()! );
						var parsed = inner.RootElement;

						if ( parsed.ValueKind == JsonValueKind.Array )
						{
							// Parsed content is a positional array — convert to named object.
							var obj = ArrayRowToObject( tableName, parsed );
							handler.Invoke( tableName, obj );
						}
						else
						{
							// Parsed content is already a named object.
							handler.Invoke( tableName, parsed.Clone() );
						}
					}
					else if ( row.ValueKind == JsonValueKind.Array )
					{
						// Raw positional array (no string wrapper).
						var obj = ArrayRowToObject( tableName, row );
						handler.Invoke( tableName, obj );
					}
					else
					{
						handler.Invoke( tableName, row.Clone() );
					}
				}
				catch ( Exception ex )
				{
					Log.Error( $"DispatchRows [{tableName}/{key}]: {ex.Message}" );
				}
			}
		}

		/// <summary>
		/// Converts a positional array row from TransactionUpdate into a named-property
		/// JsonElement so downstream code can use TryGetProperty() uniformly.
		/// Column order must match the struct field order in your SpacetimeDB module.
		/// </summary>
		private static JsonElement ArrayRowToObject( string tableName, JsonElement arr )
		{
			var values = new List<JsonElement>();
			foreach ( var v in arr.EnumerateArray() )
				values.Add( v );

			// Log the raw array so we can verify column positions.
			var preview = arr.GetRawText();
			Log.Info( $"ArrayRow [{tableName}] ({values.Count} cols): {preview[..Math.Min( preview.Length, 300 )]}" );

			// Build a JSON object string mapping column names to raw values.
			var sb = new StringBuilder( "{" );

			string[] cols = tableName switch
			{
				"player"       => new[] { "identity", "name", "score", "joined_at" },
				"chat_message" => new[] { "id", "sender", "sender_name", "text", "sent_at" },
				"ping_log"     => new[] { "id", "caller", "payload", "received_at" },
				_              => GenerateDefaultColumns( values.Count )
			};

			for ( var i = 0; i < Math.Min( cols.Length, values.Count ); i++ )
			{
				if ( i > 0 ) sb.Append( ',' );
				sb.Append( '"' );
				sb.Append( cols[i] );
				sb.Append( "\":" );
				sb.Append( values[i].GetRawText() );
			}
			sb.Append( '}' );

			return JsonDocument.Parse( sb.ToString() ).RootElement;
		}


		private static string[] GenerateDefaultColumns( int count )
		{
			var cols = new string[count];
			for ( var i = 0; i < count; i++ ) cols[i] = $"col{i}";
			return cols;
		}

		// ── IDisposable ───────────────────────────────────────────────────────

		public void Dispose()
		{
			_ws?.Dispose();
			_ws = null;
		}
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Scene component wrapper
	// ─────────────────────────────────────────────────────────────────────────

	/// <summary>
	/// Drop on a GameObject and configure in the Inspector.
	/// Calls ConnectAsync() on Start if AutoConnect is true.
	/// </summary>
	public sealed class SpacetimeDbComponent : Component
	{
		[Property] public string ServerUrl   { get; set; } = "http://localhost:8080";
		[Property] public string ModuleName  { get; set; } = "stdb";
		[Property] public bool   AutoConnect { get; set; } = true;

		public SpacetimeDbClient Client { get; private set; }

		public event Action                           OnConnected;
		public event Action<int, string>              OnDisconnected;
		public event Action<string>                   OnError;
		public event Action<string, JsonElement>      OnTableInsert;
		public event Action<string, JsonElement>      OnTableDelete;
		public event Action<string, string>           OnReducerEvent;

		protected override async void OnStart()
		{
			Client = new SpacetimeDbClient( ServerUrl, ModuleName );

			Client.OnConnected    += () => OnConnected?.Invoke();
			Client.OnDisconnected += ( c, r ) => OnDisconnected?.Invoke( c, r );
			Client.OnError        += m => OnError?.Invoke( m );
			Client.OnTableInsert  += ( t, r ) => OnTableInsert?.Invoke( t, r );
			Client.OnTableDelete  += ( t, r ) => OnTableDelete?.Invoke( t, r );
			Client.OnReducerEvent += ( r, s ) => OnReducerEvent?.Invoke( r, s );

			if ( AutoConnect )
				await Client.ConnectAsync();
		}

		protected override void OnDestroy()
		{
			Client?.Dispose();
			Client = null;
		}
	}
}
