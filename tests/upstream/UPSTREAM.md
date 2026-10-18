# Upstream test data

Copied from [prometheus/prometheus](https://github.com/prometheus/prometheus) at commit
`c052a79dc7fd0f2df2219ffe336f510b0d149b72`, directory `model/textparse/testdata`. Licensed under Apache-2.0.

`tests/upstream.rs` parses each file. Files ending in `.prom.txt` go to `parse_prometheus`,
the rest to `parse_openmetrics`. Files that don't parse yet are listed in `KNOWN_FAILURES`.

To update, change the commit below and run this from the repository root:

```sh
COMMIT=c052a79dc7fd0f2df2219ffe336f510b0d149b72
for f in $(cd tests/upstream/prometheus && ls *.txt); do
  curl -sfL "https://raw.githubusercontent.com/prometheus/prometheus/$COMMIT/model/textparse/testdata/$f" -o "tests/upstream/prometheus/$f"
done
```

New upstream files have to be added by name.

## SHA-256

```
c262ee85f66251e8c419451421a8a80f33fed8580e53d673f6eeefd9874258ec  1histogram.om.txt
3b53506eb01f5b2a979162c38763308459b3610b690ec6fccd03e4f4132b078f  alltypes.237mfs.nometa.prom.txt
612eba014d016326a0f7f13e3cfe2e8ada2cd59b2976120d84b7755291a0a977  alltypes.237mfs.prom.txt
9b7695e5135a56f90be6b5021374c61ee62c07914c61a0494b980d68f391a72f  alltypes.5mfs.om.txt
4c48d0f03056c4504bc0b8bcdada89c46cdfdb4cf0c3c76a166bfd839470bed5  alltypes.bench.om.txt
93fc80c3fc2c9683da873742b3a482e36e365a3c48376611c58ceb514d5eeb87  alltypes.bench.om2.txt
abe601efb14e60011b42f104c84c15a3c632be8fdaf21e59875f13d95d915556  alltypes.om2.txt
aad437c59e3632f65cc42271b067b83885196bf9b887dfdb9e91d318125fec80  ct.bench.om.txt
5ea9ab51b2fedc68093ebf999389c96702c1a49c654f0f3684c6c6c1d13da5b1  ct.bench.om2.txt
33c5f808a12ab525d7ca0c5473ef49c66bacfa1687763031bda171b9aab6714f  histograms.bench.om.txt
5220acfc5b0dacb71fab3e13dacccb7dc6ae9785ceb303f3bf18aa9c5e565a42  histograms.bench.om2.txt
98f987f9ad26dd8d35d097020b536ae11a6637faaf095856decb28b280ce57f8  prometheus.om1.txt
661481ca4f55321f6999fb06146bf5e520e80018f5709d4c124eb87cebc01a60  prometheus.om2.txt
```
