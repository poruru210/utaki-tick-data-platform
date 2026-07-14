#property service
#property strict

#define PROTOCOL_VERSION 1
#define HEADER_LENGTH 16
#define MAX_FRAME_BYTES 1048576
#define MAX_RECORDS 4096
#define MESSAGE_HELLO 1
#define MESSAGE_BATCH 3
#define SOURCE_SCHEMA "mt5.mqltick.v1"

void AppendU8(uchar &output[], uchar value)
  {
   int size=ArraySize(output);
   ArrayResize(output,size+1);
   output[size]=value;
  }

void AppendU16(uchar &output[], ushort value)
  {
   AppendU8(output,(uchar)(value&0xff));
   AppendU8(output,(uchar)((value>>8)&0xff));
  }

void AppendU32(uchar &output[], uint value)
  {
   AppendU8(output,(uchar)(value&0xff));
   AppendU8(output,(uchar)((value>>8)&0xff));
   AppendU8(output,(uchar)((value>>16)&0xff));
   AppendU8(output,(uchar)((value>>24)&0xff));
  }

void AppendU64(uchar &output[], ulong value)
  {
   for(int shift=0;shift<64;shift+=8)
      AppendU8(output,(uchar)((value>>shift)&0xff));
  }

void AppendI64(uchar &output[], long value)
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
   if(count<0 || count>255)
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
   int frame_size=20+payload_size;
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

bool EncodeHelloFrameV1(
   string producer_instance_id,
   string producer_session_id,
   string producer_build_id,
   string mql_compiler_build,
   string terminal_build,
   string os_contract,
   string clock_api_id,
   string campaign_id,
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
   string fields[12]={
      producer_instance_id,producer_session_id,producer_build_id,
      mql_compiler_build,terminal_build,os_contract,clock_api_id,
      campaign_id,provider_id,stable_feed_id,broker_server_fingerprint,
      exact_source_symbol
   };
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

bool EncodeBatchFrameV1(
   string session_lease_id,
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
   if(!AppendString(payload,session_lease_id))
      return false;
   if(!AppendString(payload,producer_session_id))
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

void OnStart()
  {
  }
