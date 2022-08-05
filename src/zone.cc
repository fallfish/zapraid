#include "zone.h"
#include <isa-l.h>



inline uint64_t Zone::offset2Bytes(uint64_t offset)
{
  return (mSlba << 12) + offset * Configuration::GetBlockSize();
}

void Zone::Init(Device* device, uint64_t slba, uint64_t capacity, uint64_t size)
{
  mDevice = device;
  mSlba = slba;
  mCapacity = capacity;
  mSize = size;
}

void Zone::Write(uint32_t offset, uint32_t size, void *ctx)
{
  RequestContext *reqCtx = (RequestContext*)ctx;
  if (reqCtx->append) {
    reqCtx->offset = mSize - 1;
    mDevice->Append(offset2Bytes(0), size, ctx);
  } else {
    reqCtx->offset = offset;
    mDevice->Write(offset2Bytes(offset), size, ctx);
  }
}

void Zone::Read(uint32_t offset, uint32_t size, void *ctx)
{
  mDevice->Read(offset2Bytes(offset), size, ctx);
}

void Zone::Reset(void *ctx)
{
  mDevice->ResetZone(this, ctx);
}

void Zone::Seal(void *ctx)
{
  mDevice->FinishZone(this, ctx);
}

Device* Zone::GetDevice()
{
  return mDevice;
}

uint32_t Zone::GetDeviceId()
{
  return mDevice->GetDeviceId();
}

uint32_t Zone::GetPos()
{
  return mPos;
}

void Zone::AdvancePos()
{
  mPos += 1;
}

uint32_t Zone::GetSlba()
{
  return mSlba;
}

uint32_t Zone::GetLength()
{
  return mCapacity;
}

void Zone::PrintStats()
{
  printf("device id: %d, slba: %d, length: %d\n",
      GetDeviceId(), mSlba, mCapacity);
}

void Zone::Release()
{
  mDevice->AddAvailableZone(this);
}
