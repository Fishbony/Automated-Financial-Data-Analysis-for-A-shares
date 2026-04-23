# Automated Financial Data Analysis for A-shares

# 鍚岃姳椤鸿储鍔℃暟鎹竻娲椾笌閲嶆瀯

杩欎釜椤圭洰鐢ㄤ簬澶勭悊浠庡悓鑺遍『 PC 绔洿鎺ュ鍑虹殑璐㈠姟鍘熷鏁版嵁锛岀洰鏍囨槸鎶婂師濮嬫姤琛ㄦ竻娲椼€佹爣鍑嗗寲銆佸嬀绋芥鏌ワ紝骞惰繘涓€姝ユ彁鍙栦及鍊煎垎鏋愬彲鐩存帴浣跨敤鐨勬暟鎹€?
`rawdata/` 鐩綍閲屽綋鍓嶆斁鐨勬槸 demo 鏁版嵁銆傚疄闄呬娇鐢ㄦ椂锛屽彧闇€瑕佹妸浠庡悓鑺遍『杞欢涓嬭浇涓嬫潵鐨勫師濮嬫枃浠舵斁杩?`rawdata/`锛岀劧鍚庤繍琛屼竴閿剼鏈嵆鍙€?
## 椤圭洰鑳藉仛浠€涔?
- 灏嗗悓鑺遍『瀵煎嚭鐨勫師濮?`xls` / 鏂囨湰鏁版嵁杞崲涓烘爣鍑?`csv`
- 淇濈暀鏈€杩?10 骞磋储鍔℃暟鎹?- 瀵逛笁澶ф姤琛ㄥ仛鍩虹鍕剧ń妫€鏌?- 鎻愬彇浼板€煎垎鏋愬父鐢ㄥ瓧娈?- 鐢熸垚鏍稿績璐㈠姟鎸囨爣銆佺己澶遍」鏃ュ織鍜屽彲寤烘ā缁撴灉琛?
## 鐩綍缁撴瀯

```text
.
鈹溾攢 rawdata/
鈹? 鈹溾攢 600406_debt_year.xls
鈹? 鈹溾攢 600406_benefit_year.xls
鈹? 鈹溾攢 600406_cash_year.xls
鈹? 鈹溾攢 600406_price.xls
鈹? 鈹斺攢 Info.csv
鈹溾攢 results/
鈹溾攢 01_RoyalFlushData2csv_10years.py
鈹溾攢 02_CheckStatements.py
鈹溾攢 03_ExtractCalc.py
鈹溾攢 04_FinancialCoreMetrics.py
鈹溾攢 run_pipeline.py
鈹溾攢 pipeline_utils.py
鈹溾攢 requirements.txt
鈹溾攢 environment.yml
鈹溾攢 Rebuild_BS.py
鈹溾攢 Rebuild_CF.py
鈹斺攢 Rebuild_PL.py
```

## 杈撳叆鏂囦欢瑕佹眰

璇锋妸浠庡悓鑺遍『 PC 绔鍑虹殑鍘熷鏂囦欢鏀惧叆 `rawdata/` 鐩綍锛屽苟淇濇寔濡備笅鍛藉悕瑙勫垯锛?
- `鑲＄エ浠ｇ爜_debt_year.xls`锛氳祫浜ц礋鍊鸿〃
- `鑲＄エ浠ｇ爜_benefit_year.xls`锛氬埄娑﹁〃
- `鑲＄エ浠ｇ爜_cash_year.xls`锛氱幇閲戞祦閲忚〃
- `鑲＄エ浠ｇ爜_price.xls`锛氳偂浠峰巻鍙叉暟鎹?- `Info.csv`锛氳ˉ鍏呬俊鎭枃浠讹紝鍙€?
渚嬪锛?
- `600406_debt_year.xls`
- `600406_benefit_year.xls`
- `600406_cash_year.xls`
- `600406_price.xls`

褰撳墠椤圭洰浼氳嚜鍔ㄦ牎楠岋細

- `rawdata/` 鏄惁瀛樺湪
- 鏄惁娣锋斁浜嗗鍙偂绁ㄧ殑鏁版嵁
- 鍥涗釜鏍稿績杈撳叆鏂囦欢鏄惁榻愬叏

寤鸿涓€娆″彧澶勭悊涓€鍙偂绁ㄣ€?
## 鐜鍑嗗

浣犵幇鍦ㄤ娇鐢ㄧ殑鏄?`conda` 鐜 `ds_env`锛岃繖涓」鐩凡缁忚ˉ浜嗗搴旂幆澧冩枃浠躲€?
### 鏂瑰紡涓€锛氱洿鎺ヤ娇鐢ㄧ幇鏈夌幆澧?
濡傛灉浣犵殑 `ds_env` 宸茬粡鍙敤锛屽彲浠ョ洿鎺ユ縺娲伙細

```powershell
conda activate ds_env
```

濡傛灉杩樻病瑁呬緷璧栵細

```powershell
pip install -r requirements.txt
```

### 鏂瑰紡浜岋細閫氳繃 `environment.yml` 鍒涘缓鐜

```powershell
conda env create -f environment.yml
conda activate ds_env
```

## 鎺ㄨ崘杩愯鏂瑰紡

鏈€绠€鍗曠殑鏂瑰紡鏄洿鎺ヨ繍琛屼竴閿祦绋嬶細

```powershell
python .\run_pipeline.py
```

瀹冧細鑷姩瀹屾垚锛?
1. 妫€鏌?`rawdata/` 杈撳叆鏂囦欢
2. 鍒涘缓 `results/` 鍜?`results/csv/`
3. 渚濇杩愯鍥涗釜涓昏剼鏈?4. 鍦ㄤ腑闂存煇涓€姝ュけ璐ユ椂绔嬪嵆鍋滄

## 鎵嬪姩杩愯椤哄簭

濡傛灉浣犲笇鏈涘垎姝ユ墽琛岋紝涔熷彲浠ユ寜涓嬮潰椤哄簭杩愯锛?
```powershell
python .\01_RoyalFlushData2csv_10years.py
python .\02_CheckStatements.py
python .\03_ExtractCalc.py
python .\04_FinancialCoreMetrics.py
```

## 鍚勮剼鏈綔鐢?
### `01_RoyalFlushData2csv_10years.py`

- 璇诲彇 `rawdata/` 涓殑鍚岃姳椤哄師濮嬫枃浠?- 杞崲涓夊ぇ鎶ヨ〃鍜岃偂浠锋暟鎹负 `csv`
- 娓呮礂鍗犱綅绗?- 淇濈暀鏈€杩?10 骞存暟鎹?
杈撳嚭锛?
- `results/csv/bs.csv`
- `results/csv/pl.csv`
- `results/csv/cf.csv`
- `results/csv/price.csv`

### `02_CheckStatements.py`

- 瀵逛笁澶ф姤琛ㄥ仛鍕剧ń妫€鏌?- 妫€鏌ヨ祫浜ц礋鍊哄钩琛?- 妫€鏌ョ幇閲戞祦涓夊ぇ椤瑰拰鍑€鐜伴噾鍙樺姩鍏崇郴
- 杈撳嚭妫€鏌ユ姤鍛?
### `03_ExtractCalc.py`

- 浠庡師濮嬭储鍔＄鐩腑鎻愬彇浼板€煎父鐢ㄥ瓧娈?- 瀵逛笉鍚屽悕绉扮殑绉戠洰杩涜鍊欓€夊尮閰?- 杈撳嚭涓棿鎻愬彇缁撴灉鍜屽鐞嗙粨鏋?
### `04_FinancialCoreMetrics.py`

- 璁＄畻鏍稿績鎸囨爣锛屽 Revenue銆丆FO銆丗CF銆丷OE 绛?- 杈撳嚭鍙敤浜庝及鍊煎拰寤烘ā鐨勭粨鏋滆〃
- 璁板綍缂哄け瀛楁锛屼究浜庢帓鏌?
### `Rebuild_BS.py`

- 鐢ㄤ簬杩涗竴姝ラ噸鏋勮祫浜ц礋鍊鸿〃鍙ｅ緞

### `Rebuild_CF.py` / `Rebuild_PL.py`

- 褰撳墠涓虹┖鏂囦欢锛屽睘浜庨鐣欎綅缃?
## 涓昏杈撳嚭

甯歌杈撳嚭鏂囦欢浣嶄簬 `results/` 涓嬶紝鍖呮嫭浣嗕笉闄愪簬锛?
- `results/csv/bs.csv`
- `results/csv/pl.csv`
- `results/csv/cf.csv`
- `results/csv/price.csv`
- `results/Core_Metrics.xlsx`
- `results/financial_core_metrics_plus.xlsx`
- `results/missing_items_log.csv`
- 鑻ュ共 Excel / Markdown 妫€鏌ヤ笌鍒嗘瀽鎶ュ憡

## 缂哄け椤规帓鏌?
濡傛灉鏈€缁堢粨鏋滀笉瀹屾暣锛屽彲浠ヤ紭鍏堢湅锛?
- `results/missing_items_log.csv`

杩欎釜鏂囦欢浼氬府鍔╀綘鍒ゆ柇锛?
- 鍝簺瀛楁鎴愬姛鍖归厤
- 鍝簺瀛楁娌℃湁鍦ㄥ綋鍓嶅鍑烘姤琛ㄩ噷鎵惧埌

濡傛灉鍚岃姳椤哄鍑哄瓧娈靛悕绉板拰鑴氭湰鍊欓€夊悕绉颁笉涓€鑷达紝灏遍渶瑕佽ˉ鍏呮槧灏勮鍒欍€?
## 褰撳墠椤圭洰宸茶ˉ鍏呯殑鍩虹璁炬柦

杩欐宸茬粡琛ヤ笂浜嗕笅闈㈣繖浜涘唴瀹癸細

- `requirements.txt`
- `environment.yml`
- `.gitignore`
- 涓€閿繍琛岃剼鏈?`run_pipeline.py`
- 杈撳叆鏍￠獙鍜岃緭鍑虹洰褰曞垵濮嬪寲 `pipeline_utils.py`

## 娉ㄦ剰浜嬮」

- 璇峰敖閲忎娇鐢?UTF-8 缂栫爜淇濆瓨鍚庣画鏂板鏂囦欢
- 褰撳墠椤圭洰涓殑閮ㄥ垎鏃ц剼鏈彲鑳戒粛瀛樺湪涓枃缂栫爜鍘嗗彶闂
- 濡傛灉閬囧埌瀛楁鍚嶄贡鐮佹垨鎶ヨ〃椤圭洰鍚嶄笉涓€鑷达紝浼樺厛浠庣紪鐮佸拰瀛楁鏄犲皠涓ゆ柟闈㈡帓鏌?- 寤鸿鍏堜繚鐣?`rawdata/` demo 鏁版嵁锛屾柟渚垮悗缁仛鍥炲綊娴嬭瘯
