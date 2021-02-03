sgx_status_t verify_lproof(uint8_t *lproof, uint32_t lproof_size, uint64_t source_tx, uint64_t target_tx, uint8_t *source_hash, uint8_t *target_hash)
{
  if (source_tx == 0 || source_tx > target_tx)
  {
    return SGX_ERROR_UNEXPECTED;
  }

  uint8_t acc_hash[32];
  memcpy(acc_hash, lproof, 32);

  if (memcmp(source_hash, acc_hash, 32) != 0)
  {
    return SGX_ERROR_UNEXPECTED;
  }

  int size = int(target_tx - source_tx) + 1;

  for (int i = 1; i < size; i++)
  {
    uint8_t bs[8 + 2*32];

    encode_long(source_tx + uint64_t(i), bs);
    memcpy(bs+8, acc_hash, 32);
    memcpy(bs+8+32, lproof+i*32, 32);

    sgx_status_t stat;
    stat = sgx_sha256_msg((const uint8_t *) bs, 8 + 2*32, &acc_hash);

    if (stat != SGX_SUCCESS)
    {
      return stat;
    }
  }

  if (memcmp(target_hash, acc_hash, 32) != 0)
  {
    return SGX_ERROR_UNEXPECTED;
  }

  return SGX_SUCCESS;
}