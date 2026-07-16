# M4 final audit checklist

2026-07-16時点では外部gate未実施のため、これはpass receiptではなく未完了チェックリストです。
外部証跡は [`m4-external-evidence-template.md`](m4-external-evidence-template.md) の
secret-free summaryと、repository外のraw artifact digestを突合してから判定します。

```yaml
delivery_status: incomplete
final_audit: pending
required_action_count: 6
```

## Audit items

- [x] M4-1〜M4-6のcontract、implementation、focused test、GPT-5.5 xhigh reviewを確認した。
- [x] M4-7のnetwork-free fault/load/repository evidenceとLinux-equivalent Race artifactを記録した。
- [x] Linux相当でM4 package race artifactを取得し、8 packageのpass、failなし、`DATA RACE`なしを確認した。
- [ ] isolated real-R2 raw/read smokeをpassさせる。
- [ ] `m4_real_r2_smoke`のprepare/verifyでreal-R2 handover、old credential revoke、new writer、
      separate read-only verificationをpassさせる。
- [ ] 24時間以上のlive MT5 soakとfault event/recovery artifactを取得する。
- [ ] soak後のempty-cache/read-only independent verificationをpassさせる。
- [ ] secret scan、scope exclusion、artifact retention、runbook executionを再確認する。
- [ ] production prune CLIをstrict retention config、完全なscope binding、durable wall-clock watermark、
      bounded remote observation、canonical retention proof、frozen plan time付きで隔離real R2にて再検証する。

required actionがzeroになるまで、`delivery_status: completed`や`final_audit: pass`を記録しない。
