# Strategic Radar collector

Strategic Radar'ın haber ve think tank kaynaklarını Claude yerine toplayan küçük program. GitHub Actions üzerinde iki saatte bir, ücretsiz çalışır. RSS beslemelerini ve GDELT haber veritabanını okur, yalnızca güvenilir alan adlarını tutar, haberleri dokuz dosyaya (turkey, syria, iran, israel, gulf, pkk, mideast, russia, europe) ayırır, kaynağa Admiralty güvenilirlik harfini verir, tekrarları ayıklar ve sonucu `data` dalına JSON olarak yazar. Claude görevleri bu dosyayı tek istekte okur, yalnızca doğrulama için birkaç sayfa açar.

## Çıktılar (`data` dalı)

| Dosya | İçerik |
|---|---|
| `latest.json` | Son 48 saatin haberleri, dosyalara göre gruplu, en yeni başta |
| `files/<id>.json` | Aynı veri, her dosya ayrı |
| `thinktanks.json` | Son 5 günün think tank yayınları |
| `status.json` | Her beslemenin ve GDELT sorgularının sağlık durumu |
| `archive/<tarih>.jsonl` | Her yeni haber, 30 gün saklanır |
| `state.json`, `store.json` | Programın çalışmalar arası hafızası |

`data` dalı her çalışmada tek bir commit olarak yeniden yazılır, bu yüzden depo büyümez.

## Kurulum (yaklaşık 10 dakika)

1. github.com'da oturum açın. Sağ üstteki **+** menüsünden **New repository** seçin. Ad: `strategic-radar-collector`. Görünürlük: **Public**. "Add a README" kutusunu işaretlemeyin. **Create repository**.
2. Açılan sayfada **uploading an existing file** bağlantısına tıklayın. Bu klasördeki her şeyi (gizli `.github` klasörü dahil) sürükleyip bırakın. Not: tarayıcı `.github` klasörünü göstermeyebilir; o durumda 3. adımdaki yolu izleyin.
3. `.github/workflows/collect.yml` yüklenmediyse: depoda **Add file → Create new file** seçin, dosya adına `.github/workflows/collect.yml` yazın, bu klasördeki aynı dosyanın içeriğini yapıştırın, **Commit changes**.
4. Depoda **Settings → Actions → General** sayfasına gidin. "Workflow permissions" altında **Read and write permissions** seçin, **Save**.
5. **Actions** sekmesine geçin. Gerekirse "I understand my workflows, go ahead and enable them" düğmesine basın. Soldan **collect** iş akışını seçin, **Run workflow** ile ilk çalışmayı elle başlatın. 2-4 dakika sürer.
6. Çalışma yeşil bittikten sonra depo adresinizdeki `data` dalını açın; `status.json` dosyası hangi beslemelerin çalıştığını gösterir.
7. Claude'a şu adresi verin: `https://raw.githubusercontent.com/<kullanıcı-adınız>/strategic-radar-collector/data`. Ben bunu Strategic Radar'ın ayarlarına (`meta/config.collectorBase`) yazacağım ve görevler o andan itibaren toplayıcıyı kullanacak.

## Bakım

- **Kaynak eklemek veya çıkarmak:** `collector/config/sources.yaml` dosyasını GitHub üzerinde düzenleyin. Bir beslemenin `url` alanı boşsa program sitenin ana sayfasından beslemeyi kendisi bulmaya çalışır. Üç kez üst üste düşen besleme de yeniden aranır.
- **Sınıflama kelimeleri:** `collector/config/classify.yaml`.
- **Engelli alan adları:** `sources.yaml` içindeki `blocked_domains` listesi Strategic Radar'ın kaynak politikasıyla aynı tutulmalı. Claude görevleri yazmadan önce canlı politikayı yine de kontrol eder.
- **60 gün kuralı:** GitHub, açık depolarda 60 gün boyunca ana dalda hiçbir değişiklik olmazsa zamanlanmış iş akışlarını durdurur ve önceden e-posta atar. E-postadaki bağlantıyla ya da Actions sekmesindeki **Enable workflow** düğmesiyle tek tıkla yeniden açılır. Kaynak listesinde arada bir yapılan küçük bir düzenleme de bunu önler.
- **Yerelde deneme:** `pip install -r requirements.txt` ve `python collector/collect.py --data out`. Ağ olmadan test için: `python collector/collect.py --data out --offline-fixtures tests/fixtures`.

## Maliyet

GitHub Actions açık depolarda ücretsizdir. Her çalışma 2-4 dakika sürer; günde 12 çalışma yapılır.
