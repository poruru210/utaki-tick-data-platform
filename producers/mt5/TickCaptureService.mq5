#property service
#property strict

#define PROTOCOL_VERSION 1
#define HEADER_LENGTH 16
#define MIN_FRAME_BYTES 20
#define MAX_FRAME_BYTES 1048576
#define MAX_RECORDS 4096
#define MAX_STRING_BYTES 255
#define MESSAGE_HELLO 1
#define MESSAGE_RESUME 2
#define MESSAGE_BATCH 3
#define MESSAGE_ACK 4
#define MESSAGE_ERROR 5
#define ACK_ACCEPTED_ADVANCED 1
#define ACK_ACCEPTED_NO_ADVANCE 2
#define ACK_DUPLICATE 3
#define ACK_DENSE_BOUNDARY 4
#define ACK_DENSE_UNRESOLVED 5
#define ACK_RETRYABLE_ERROR 6
#define ACK_FATAL_PROTOCOL_ERROR 7
#define ACK_SOURCE_STATE_CONFLICT 8
#define ACK_SESSION_LEASE_CONFLICT 9
#define SOURCE_SCHEMA "mt5.mqltick.v1"

input string InpGatewayHost="127.0.0.1";
input ushort InpGatewayPort=17001;
input string InpProducerInstanceID="mt5-local-01";
input string InpProducerBuildID="tick-capture-mt5-v1";
input string InpMQLCompilerBuild="metaeditor-build";
input string InpOSContract="windows-loopback-tcp";
input string InpClockAPIID="TimeGMT+GetMicrosecondCount";
input string InpProviderID="broker-provider";
input string InpStableFeedID="broker-feed-01";
input string InpBrokerServerFingerprint="broker-server-local-01";
input string InpExactSourceSymbol="EURUSD";
input long InpInitialFromMSC=0;
input uint InpInitialBatchCount=128;
input uint InpReconnectBackoffMS=1000;
input uint InpConnectTimeoutMS=3000;
input uint InpACKTimeoutMS=10000;

struct AckResult
  {
   string producer_session_id;
   ulong batch_sequence;
   uchar status;
   long committed_cursor_msc;
   long next_from_msc;
   uint next_requested_count;
  };

void AppendU8(uchar &output[],uchar value)
  {
   int size=ArraySize(output);
   ArrayResize(output,size+1);
   output[size]=value;
  }

void AppendU16(uchar &output[],ushort value)
  {
   AppendU8(output,(uchar)(value&0xff));
   AppendU8(output,(uchar)((value>>8)&0xff));
  }

void AppendU32(uchar &output[],uint value)
  {
   AppendU8(output,(uchar)(value&0xff));
   AppendU8(output,(uchar)((value>>8)&0xff));
   AppendU8(output,(uchar)((value>>16)&0xff));
   AppendU8(output,(uchar)((value>>24)&0xff));
  }

void AppendU64(uchar &output[],ulong value)
  {
   for(int shift=0;shift<64;shift+=8)
      AppendU8(output,(uchar)((value>>shift)&0xff));
  }

void AppendI64(uchar &output[],long value)
  {
   AppendU64(output,(ulong)value);
  }

void AppendBytes(uchar &output[],const uchar &source[])
  {
   for(int i=0;i<ArraySize(source);i++)
      AppendU8(output,source[i]);
  }

bool AppendString(uchar &output[],string value)
  {
   uchar bytes[];
   int count=StringToCharArray(value,bytes,0,WHOLE_ARRAY,CP_UTF8);
   if(count>0 && bytes[count-1]==0)
      count--;
   if(count<0 || count>MAX_STRING_BYTES)
      return false;
   AppendU16(output,(ushort)count);
   for(int i=0;i<count;i++)
      AppendU8(output,bytes[i]);
   return true;
  }

union DoubleBits
  {
   double value;
   long bits;
  };

void AppendDoubleBits(uchar &output[],double value)
  {
   DoubleBits converter;
   converter.value=value;
   AppendU64(output,(ulong)converter.bits);
  }

uint ReadU32(const uchar &data[],int offset)
  {
   return (uint)data[offset]
      | ((uint)data[offset+1]<<8)
      | ((uint)data[offset+2]<<16)
      | ((uint)data[offset+3]<<24);
  }

ushort ReadU16(const uchar &data[],int offset)
  {
   return (ushort)((ushort)data[offset]|((ushort)data[offset+1]<<8));
  }

ulong ReadU64(const uchar &data[],int offset)
  {
   ulong value=0;
   for(int shift=0;shift<64;shift+=8)
      value|=((ulong)data[offset+(shift/8)])<<shift;
   return value;
  }

long ReadI64(const uchar &data[],int offset)
  {
   return (long)ReadU64(data,offset);
  }

bool ReadString(const uchar &data[],int &offset,string &value)
  {
   if(offset+2>ArraySize(data))
      return false;
   ushort size=ReadU16(data,offset);
   offset+=2;
   if(size>MAX_STRING_BYTES || offset+(int)size>ArraySize(data))
      return false;
   uchar bytes[];
   ArrayResize(bytes,(int)size);
   for(int i=0;i<(int)size;i++)
      bytes[i]=data[offset+i];
   value=CharArrayToString(bytes,0,(int)size,CP_UTF8);
   offset+=(int)size;
   return true;
  }

uint CRC32C(const uchar &data[])
  {
   uint crc=0xffffffff;
   for(int i=0;i<ArraySize(data);i++)
     {
      crc^=(uint)data[i];
      for(int bit=0;bit<8;bit++)
        {
         if((crc&1)!=0)
            crc=(crc>>1)^0x82f63b78;
         else
            crc>>=1;
        }
     }
   return crc^0xffffffff;
  }

bool BuildFrame(ushort message_type,const uchar &payload[],uchar &frame[])
  {
   int payload_size=ArraySize(payload);
   int frame_size=MIN_FRAME_BYTES+payload_size;
   if(frame_size>MAX_FRAME_BYTES)
      return false;
   ArrayResize(frame,0);
   AppendU8(frame,0x54);
   AppendU8(frame,0x49);
   AppendU8(frame,0x43);
   AppendU8(frame,0x4b);
   AppendU16(frame,PROTOCOL_VERSION);
   AppendU16(frame,message_type);
   AppendU32(frame,(uint)frame_size);
   AppendU32(frame,HEADER_LENGTH);
   AppendBytes(frame,payload);
   AppendU32(frame,CRC32C(frame));
   return true;
  }

bool EncodeHelloFrameV1(string producer_instance_id,
                        string producer_session_id,
                        string producer_build_id,
                        string mql_compiler_build,
                        string terminal_build,
                        string os_contract,
                        string clock_api_id,
                        string provider_id,
                        string stable_feed_id,
                        string broker_server_fingerprint,
                        string exact_source_symbol,
                        uchar acquisition_mode,
                        long initial_from_msc,
                        uint capability_flags,
                        uchar &frame[])
  {
   uchar payload[];
   string fields[11]={producer_instance_id,producer_session_id,producer_build_id,
      mql_compiler_build,terminal_build,os_contract,clock_api_id,
      provider_id,stable_feed_id,broker_server_fingerprint,exact_source_symbol};
   for(int i=0;i<ArraySize(fields);i++)
      if(!AppendString(payload,fields[i]))
         return false;
   if(!AppendString(payload,SOURCE_SCHEMA))
      return false;
   if(acquisition_mode!=1 && acquisition_mode!=2)
      return false;
   AppendU8(payload,acquisition_mode);
   AppendI64(payload,initial_from_msc);
   AppendU32(payload,capability_flags);
   return BuildFrame(MESSAGE_HELLO,payload,frame);
  }

bool EncodeBatchFrameV1(string session_lease_id,
                        string producer_session_id,
                        ulong batch_sequence,
                        long requested_from_msc,
                        uint requested_count,
                        long fetch_wall_start_s,
                        long fetch_wall_end_s,
                        ulong fetch_monotonic_start_us,
                        ulong fetch_monotonic_end_us,
                        int returned_count,
                        int copy_ticks_error,
                        uint source_status_flags,
                        const MqlTick &ticks[],
                        ulong first_capture_sequence,
                        uchar &frame[])
  {
   int record_count=ArraySize(ticks);
   if(record_count>MAX_RECORDS)
      return false;
   if(returned_count>=0 && returned_count!=record_count)
      return false;
   if(returned_count<0 && record_count!=0)
      return false;
   uchar payload[];
   if(!AppendString(payload,session_lease_id) || !AppendString(payload,producer_session_id))
      return false;
   AppendU64(payload,batch_sequence);
   AppendI64(payload,requested_from_msc);
   AppendU32(payload,requested_count);
   AppendI64(payload,fetch_wall_start_s);
   AppendI64(payload,fetch_wall_end_s);
   AppendU64(payload,fetch_monotonic_start_us);
   AppendU64(payload,fetch_monotonic_end_us);
   AppendU32(payload,(uint)returned_count);
   AppendU32(payload,(uint)copy_ticks_error);
   AppendU32(payload,source_status_flags);
   if(!AppendString(payload,SOURCE_SCHEMA))
      return false;
   AppendU32(payload,(uint)record_count);
   for(int i=0;i<record_count;i++)
     {
      AppendI64(payload,(long)ticks[i].time);
      AppendDoubleBits(payload,ticks[i].bid);
      AppendDoubleBits(payload,ticks[i].ask);
      AppendDoubleBits(payload,ticks[i].last);
      AppendU64(payload,ticks[i].volume);
      AppendI64(payload,ticks[i].time_msc);
      AppendU32(payload,(uint)ticks[i].flags);
      AppendDoubleBits(payload,ticks[i].volume_real);
      AppendU64(payload,first_capture_sequence+(ulong)i);
     }
   return BuildFrame(MESSAGE_BATCH,payload,frame);
  }

bool SendAll(int socket,const uchar &frame[])
  {
   int offset=0;
   int total=ArraySize(frame);
   while(offset<total && !IsStopped())
     {
      int remaining=total-offset;
      uchar part[];
      ArrayResize(part,remaining);
      for(int i=0;i<remaining;i++)
         part[i]=frame[offset+i];
      int sent=SocketSend(socket,part,(uint)remaining);
      if(sent<=0 || sent>remaining)
         return false;
      offset+=sent;
     }
   return offset==total;
  }

bool ReadExact(int socket,uchar &output[],int wanted)
  {
   if(wanted<0 || wanted>MAX_FRAME_BYTES)
      return false;
   ArrayResize(output,wanted);
   int offset=0;
   while(offset<wanted && !IsStopped())
     {
      uchar part[];
      int received=SocketRead(socket,part,(uint)(wanted-offset),InpACKTimeoutMS);
      if(received<=0 || received>wanted-offset)
         return false;
      for(int i=0;i<received;i++)
         output[offset+i]=part[i];
      offset+=received;
     }
   return offset==wanted;
  }

bool ReadFrame(int socket,uchar &frame[])
  {
   uchar header[];
   if(!ReadExact(socket,header,16))
      return false;
   if(ReadU32(header,12)!=(uint)HEADER_LENGTH)
      return false;
   uint frame_length=ReadU32(header,8);
   if(frame_length<MIN_FRAME_BYTES || frame_length>MAX_FRAME_BYTES)
      return false;
   ArrayResize(frame,(int)frame_length);
   for(int i=0;i<16;i++)
      frame[i]=header[i];
   uchar tail[];
   if(!ReadExact(socket,tail,(int)frame_length-16))
      return false;
   for(int i=0;i<ArraySize(tail);i++)
      frame[16+i]=tail[i];
   if(frame[0]!=0x54 || frame[1]!=0x49 || frame[2]!=0x43 || frame[3]!=0x4b)
      return false;
   if(ReadU16(frame,4)!=PROTOCOL_VERSION)
      return false;
   uint stored=ReadU32(frame,(int)frame_length-4);
   uchar body[];
   ArrayResize(body,(int)frame_length-4);
   for(int i=0;i<ArraySize(body);i++)
      body[i]=frame[i];
   return stored==CRC32C(body);
  }

bool ReadResume(int socket,string &session_lease_id,long &next_from_msc,uint &next_requested_count)
  {
   uchar frame[];
   if(!ReadFrame(socket,frame) || ReadU16(frame,6)!=MESSAGE_RESUME)
      return false;
   int offset=16;
   ushort accepted=ReadU16(frame,offset);
   offset+=2;
   if(accepted!=PROTOCOL_VERSION)
      return false;
   // A ResumeV1 contains gateway_instance_id before session_lease_id.
   offset=16+2;
   string gateway_instance_id;
   if(!ReadString(frame,offset,gateway_instance_id) || !ReadString(frame,offset,session_lease_id))
      return false;
   if(offset+8+32+8+32+8+4+4+4+4>ArraySize(frame)-4)
      return false;
   offset+=8+32+8+32;
   next_from_msc=ReadI64(frame,offset);
   offset+=8;
   next_requested_count=ReadU32(frame,offset);
   return next_requested_count>0;
  }

bool ReadAck(int socket,ulong expected_sequence,AckResult &result,bool &retryable)
  {
   retryable=true;
   uchar frame[];
   if(!ReadFrame(socket,frame))
      return false;
   retryable=false;
   ushort message_type=ReadU16(frame,6);
   int offset=16;
   if(message_type==MESSAGE_ERROR)
     {
      if(offset+2+1+2+8+2>ArraySize(frame)-4)
         return false;
      ushort code=ReadU16(frame,offset);
      offset+=2;
      retryable=frame[offset]!=0;
      Print("Gateway ErrorV1 code=",code," retryable=",retryable);
      return false;
     }
   if(message_type!=MESSAGE_ACK)
      return false;
   if(!ReadString(frame,offset,result.producer_session_id))
      return false;
   if(offset+8+32+8+1+8+32+8+4+4>ArraySize(frame)-4)
      return false;
   result.batch_sequence=ReadU64(frame,offset);
   offset+=8+32+8;
   result.status=frame[offset];
   offset++;
   result.committed_cursor_msc=ReadI64(frame,offset);
   offset+=8+32;
   result.next_from_msc=ReadI64(frame,offset);
   offset+=8;
   result.next_requested_count=ReadU32(frame,offset);
   if(result.batch_sequence!=expected_sequence)
      return false;
   if(result.status==ACK_RETRYABLE_ERROR || result.status==ACK_SESSION_LEASE_CONFLICT)
     {
      retryable=true;
      return false;
     }
   return result.status>=ACK_ACCEPTED_ADVANCED && result.status<=ACK_SESSION_LEASE_CONFLICT;
  }

int ConnectGateway()
  {
   int socket=SocketCreate();
   if(socket==INVALID_HANDLE)
      return INVALID_HANDLE;
   if(!SocketConnect(socket,InpGatewayHost,InpGatewayPort,InpConnectTimeoutMS))
     {
      Print("SocketConnect failed error=",GetLastError());
      SocketClose(socket);
      return INVALID_HANDLE;
     }
   return socket;
  }

string NewProducerSessionID()
  {
   return "session-"+IntegerToString((long)TimeLocal())+"-"+IntegerToString((long)GetTickCount64());
  }

bool SendHelloAndReadResume(int socket,string producer_session_id,string &session_lease_id,long &next_from_msc,uint &next_requested_count)
  {
   uchar hello[];
   string terminal_build=IntegerToString((long)TerminalInfoInteger(TERMINAL_BUILD));
   if(!EncodeHelloFrameV1(InpProducerInstanceID,producer_session_id,InpProducerBuildID,
      InpMQLCompilerBuild,terminal_build,InpOSContract,InpClockAPIID,
      InpProviderID,InpStableFeedID,InpBrokerServerFingerprint,InpExactSourceSymbol,
      1,InpInitialFromMSC,0,hello))
      return false;
   if(!SendAll(socket,hello))
      return false;
   return ReadResume(socket,session_lease_id,next_from_msc,next_requested_count);
  }

void OnStart()
  {
   string producer_session_id=NewProducerSessionID();
   string session_lease_id="";
   long next_from_msc=InpInitialFromMSC;
   uint next_requested_count=InpInitialBatchCount;
   ulong batch_sequence=1;
   ulong capture_sequence=1;
   int socket=INVALID_HANDLE;
   bool have_inflight=false;
   bool inflight_empty=false;
   uchar inflight[];
   while(!IsStopped())
     {
      if(socket==INVALID_HANDLE || !SocketIsConnected(socket))
        {
         if(socket!=INVALID_HANDLE)
            SocketClose(socket);
         socket=ConnectGateway();
         if(socket==INVALID_HANDLE)
           {
            Sleep(InpReconnectBackoffMS);
            continue;
           }
         if(!SendHelloAndReadResume(socket,producer_session_id,session_lease_id,next_from_msc,next_requested_count))
           {
            SocketClose(socket);
            socket=INVALID_HANDLE;
            Sleep(InpReconnectBackoffMS);
            continue;
           }
         Print("Gateway resumed from msc=",next_from_msc," count=",next_requested_count);
        }

      if(!have_inflight)
        {
         MqlTick ticks[];
         long fetch_wall_start_s=(long)TimeGMT();
         ulong fetch_monotonic_start_us=GetMicrosecondCount();
         ResetLastError();
         ulong request_from=(next_from_msc<0 ? 0 : (ulong)next_from_msc);
         int received=CopyTicks(InpExactSourceSymbol,ticks,COPY_TICKS_ALL,request_from,next_requested_count);
         int copy_ticks_error=GetLastError();
         ulong fetch_monotonic_end_us=GetMicrosecondCount();
         long fetch_wall_end_s=(long)TimeGMT();
         if(!EncodeBatchFrameV1(session_lease_id,producer_session_id,batch_sequence,
            next_from_msc,next_requested_count,fetch_wall_start_s,fetch_wall_end_s,
            fetch_monotonic_start_us,fetch_monotonic_end_us,received,copy_ticks_error,
            0,ticks,capture_sequence,inflight))
           {
            Print("BatchFrameV1 encoding failed; stopping service");
            break;
           }
         if(received>0)
            capture_sequence+=(ulong)received;
         inflight_empty=(received<=0);
         have_inflight=true;
        }

      if(!SendAll(socket,inflight))
        {
         SocketClose(socket);
         socket=INVALID_HANDLE;
         Sleep(InpReconnectBackoffMS);
         continue;
        }
      AckResult ack;
      bool retryable=false;
      if(!ReadAck(socket,batch_sequence,ack,retryable))
        {
         SocketClose(socket);
         socket=INVALID_HANDLE;
         if(!retryable)
           {
            Print("Gateway returned a non-retryable response; stopping service");
            break;
           }
         Sleep(InpReconnectBackoffMS);
         continue;
        }
      if(ack.status==ACK_DENSE_UNRESOLVED || ack.status==ACK_SOURCE_STATE_CONFLICT || ack.status==ACK_FATAL_PROTOCOL_ERROR)
        {
         Print("Gateway returned terminal AckV1 status=",ack.status,"; stopping service");
         break;
        }
      next_from_msc=ack.next_from_msc;
      next_requested_count=ack.next_requested_count;
      have_inflight=false;
      ArrayResize(inflight,0);
      batch_sequence++;
      if(inflight_empty)
         Sleep(250);
      inflight_empty=false;
     }
   if(socket!=INVALID_HANDLE)
      SocketClose(socket);
  }
