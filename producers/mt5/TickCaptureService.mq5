#property service
#property strict

// M0 scaffold for the MT5 producer.
//
// The implementation will be limited to:
//   CopyTicks(COPY_TICKS_ALL)
//   lossless mt5.mqltick.v1 encoding
//   loopback TCP handshake, send, ACK, and reconnect
//
// It must not own a WAL, SQLite state, Parquet writer, R2 client, or trading
// call. Exact wire offsets are frozen in protocol/v1 before implementation.
void OnStart()
  {
   // Intentionally empty until the V1 contract is frozen.
  }
