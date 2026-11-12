# Kafka's Hoard

Archivador local de papeleo: lee tus facturas, contratos, pólizas, recibos y cartas oficiales, extrae los importes y fechas que importan, los convierte en plazos y te avisa antes de cada uno.

[English version](README.md)

## Qué es

Kafka's Hoard lee facturas, contratos, pólizas de seguro, recibos, garantías, cartas de Hacienda, de Tráfico o del ayuntamiento, multas, informes de la ITV y documentos de identidad. Averigua quién emitió cada uno, los importes y cada fecha con su papel, convierte esas fechas en plazos (contando días hábiles españoles donde la ley lo hace), te avisa antes de cada uno y responde preguntas citando el documento y la página.

Es una app de la familia Hoard. Funciona en tu PC, guarda sus datos en `data/`, trabaja sola en el navegador y puede manejarla un asistente por MCP.

## Qué hace

**Lectura de documentos**
- Arrastra archivos a la página, déjalos en una carpeta vigilada (por defecto `data/inbox`) o deja que lea la cuenta de correo configurada en Faustus o la pasarela de correo del hub: los adjuntos PDF e imagen se archivan y la contraseña del correo nunca sale de Faustus.
- Formatos: PDF (capa de texto, con OCR para páginas escaneadas si está instalado el paquete opcional de OCR), imágenes (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, `.pptx` (una página por diapositiva, con notas del orador), HTML y texto plano. Cada archivo se guarda una sola vez por SHA-256 y el original nunca se modifica.
- Unas reglas, sin necesidad de modelo, clasifican el documento (factura, recibo, suministro, contrato, seguro, garantía, impuestos, notificación oficial, multa, vehículo/ITV, identidad, suscripción, nómina, banco, manual de instrucciones), nombran al emisor, eligen la referencia, el importe y el periodo de facturación y encuentran cada fecha con su papel (emisión, vencimiento, renovación, efecto, compra, entrega, caducidad, notificación, próxima ITV, fin de permanencia). Cada dato conserva su evidencia: la cita y la página.
- Los documentos dudosos y los correos que podrían ser papeleo van a una lista de revisión: aceptar, cambiar o ignorar. Tu edición de cualquier campo siempre gana al releer.
- El primer escaneo del correo y los archivos antiguos se archivan sin avisos y sus plazos pasados se crean como hechos, de modo que solo oyes hablar de lo que aún está por venir.

**Plazos y recordatorios**
- Vencimientos de pago, renovaciones, último día para cancelar una renovación, fin de la garantía legal de 3 años, fin de un periodo de permanencia, los 20 días naturales para pagar una multa de tráfico con descuento o presentar alegaciones, plazos de respuesta a la administración en días hábiles, caducidad del DNI y próxima ITV. «¿Por qué esta fecha?» muestra la regla, la fecha base, los días contados y la cita; véase [docs/RULES.md](docs/RULES.md).
- Días de antelación por tipo de plazo que puedes cambiar. Un aviso por plazo y pasada, los vencidos una vez, nada por la noche (23:00-07:00 por defecto; los urgentes pueden pasar).
- Canales: aviso de Windows, el hub de la familia Hoard, ntfy, Telegram y correo. Los plazos recurrentes pasan a la siguiente ocurrencia al marcarlos como hechos.
- Los calendarios de días hábiles cubren los festivos nacionales y las comunidades de Madrid, Cataluña, Andalucía, Valencia, Galicia y País Vasco, más los festivos extra que añadas.
- Otras apps de la familia (por ejemplo, mantenimiento del hogar) envían sus plazos con un origen y una clave externa; reenviar actualiza en lugar de duplicar, y lo que cambias o descartas aquí prevalece.

**Series, garantías y búsqueda**
- Facturas, seguros y suscripciones del mismo emisor y referencia forman una serie con historial de precios; un cambio de precio del 5 % o más (configurable) genera un aviso.
- La búsqueda de texto completo (SQLite FTS5, sin acentos) en todas las páginas devuelve fragmentos con documento y página, opcionalmente limitada a ciertos documentos. `warranty_check` dice si una compra sigue en garantía y hasta cuándo.
- Los manuales de instrucciones se guardan y se pueden buscar, y nunca crean un plazo.
- Comparar dos versiones archivadas de un documento con citas a ambos ([docs/DOCUMENT_COMPARE.md](docs/DOCUMENT_COMPARE.md)).
- Dosieres: para una pregunta y un conjunto de documentos archivados, pasajes con citas `[d_id · p. N · l. M]` más fechas, importes, duraciones y obligaciones leídos con reglas fijas; las contradicciones se listan lado a lado y nunca se resuelven por ti. Un dosier se marca como caducado cuando cambia una fuente, y una comparación de requisitos alinea las cláusulas de dos versiones ([docs/DOSSIER.md](docs/DOSSIER.md)).

**Enlaces con otras apps**
- Las facturas y recibos con importe y fecha se cotejan con el libro de cuentas; cuando exactamente un movimiento puntúa 0,8 o más se enlaza, y puedes elegir uno a mano si ninguno está claro.
- Las tareas con fecha de actas de reuniones que son tuyas se convierten en plazos; las compras entregadas del seguimiento de envíos crean sus plazos de garantía.
- Una carpeta de la declaración de la renta del año natural: el papeleo habitual, un índice, un CSV, el resumen anual del libro de cuentas y la lista de certificados que faltan.

**Taller**
- Unir PDF, dividir por página, rangos o cada N páginas, extraer, borrar, girar o reordenar páginas, comprimir (Ghostscript si está instalado, si no un compresor propio; opcionalmente hasta un tamaño objetivo), poner o quitar contraseña (AES-256), estampar una marca de agua de texto, editar título, autor, asunto y palabras clave, convertir fotos o Word, ODT y RTF a PDF, convertir páginas de PDF en imágenes, exportar Markdown estructurado y tablas a XLSX o CSV.
- Kafka incluye su propio motor PDF nativo (`native/pdf`, se compila con `python scripts/build_pdf_engine.py` y necesita Rust). Sin él, solo las operaciones PDF no están disponibles.
- Los resultados traen tamaños antes y después y botón de descarga; los resultados PDF pueden archivarse en Kafka. El taller nunca edita un archivo en el sitio.

**Lectura de documentos para la familia**
- Otras apps llaman a Kafka por el hub para extraer texto y hacer OCR y pasan rutas, nunca bytes: `doc_extract` (PDF, imágenes, `.docx`, `.eml`, HTML, texto, hojas de cálculo, archivos comprimidos), `ocr_image`, `ocr_file`, `ocr_pdf`, `ocr_engine`, `ocr_status` y `doc_structure`.
- Motores de OCR: `indra`, `winocr`, `rapidocr`, `light-ocr` y `tesseract`, elegidos con el ajuste «Motor de OCR» (`auto`, `indra` o `current`); si el motor elegido falla en una imagen, la lee otro. Sin paquete de OCR, las páginas escaneadas se guardan y se marcan para revisión.

**Privacidad**
- Los números de DNI, IBAN, tarjeta y teléfono se enmascaran en lo que devuelve MCP salvo que pidas expresamente ese número. La interfaz local lo muestra todo.

## Pantallas

En español por defecto, en inglés con un clic, tema oscuro.

- **Plazos**: vencidos, esta semana, próximos 30 días y más adelante; marcar como hecho, posponer, editar, descartar, añadir a mano; novedades desde la última visita.
- **Documentos**: subida arrastrando archivos a cualquier parte, lista con filtros por tipo y estado, búsqueda de texto completo con fragmentos.
- **Detalle**: vista previa de la página, datos extraídos con evidencia, plazos con «¿por qué esta fecha?», series e historial de precios, movimiento enlazado del libro de cuentas.
- **Revisión**: documentos y correos de los que las reglas no estaban seguras.
- **Taller**: las operaciones del taller, cada una con zona de arrastre, opciones y tarjeta de resultado; más la tarjeta de la carpeta de la renta.
- **Dosieres**: crear, refrescar y comparar dosieres.
- **Ajustes**: carpetas del taller y vigiladas, origen del correo, comunidad del calendario, días de antelación, ruta de avisos y canales con botón de prueba, estado del OCR y del modelo, idioma, actividad reciente.

## Cómo se ejecuta

Requiere Python 3.11+, Node 22 solo para recompilar la interfaz y Faustus con una cuenta de correo para la lectura automática del correo.

```bash
python -m venv venv
venv/Scripts/python -m pip install -r requirements.txt      # Windows; venv/bin/python en otros sistemas
venv/Scripts/python -m kafka_hoard                            # http://127.0.0.1:5200
```

La interfaz ya está compilada en `kafka_hoard/static`; para recompilarla ejecuta `npm install && npx vite build`. `python scripts/launch.py` arranca la app en un puerto libre y abre el navegador. El OCR es opcional: `pip install ".[ocr]"`.

| Variable | Para qué sirve |
| --- | --- |
| `KAFKA_PORT` | Puerto (por defecto `5200`). |
| `PORT_STRICT=1` | No pasar a otro puerto. |
| `KAFKA_DATA_DIR` | Carpeta de datos (por defecto `./data`). |
| `KAFKA_ALLOWED_HOSTS` | Nombres de host extra aceptados tras un túnel. |
| `KAFKA_SCHEDULER=0` | Sin trabajo en segundo plano. |
| `KAFKA_OFFLINE=1` | Sin red. |
| `KAFKA_OCR=0`, `KAFKA_OCR_BACKEND` | Desactivar el OCR o elegir el motor. |
| `KAFKA_TELEGRAM_TOKEN`, `KAFKA_TELEGRAM_CHAT_ID`, `KAFKA_NTFY_TOPIC`, `KAFKA_SMTP_*`, `KAFKA_FAUSTUS_DIR` | Secretos de avisos y correo; pueden ir en `.env` (véase `.env.example`) o en Ajustes. |

Todo lo demás es un ajuste guardado en `data/kafka.db`. Los Hoards se ejecutan bajo demanda: el hub de la familia puede listar, arrancar y parar este.

## Conexión con Faustus y la familia Hoard

- **MCP**: `mcp_server.py` es un puente MCP por stdio llamado `kafka-hoard`. Nunca abre la base de datos: reenvía cada llamada a la app en marcha con el token de `data/mcp-token` y arranca la app si no responde. `faustus-plugin.json` describe la app, su comprobación de salud y el puente. El catálogo completo, con argumentos, está en [docs/API.md](docs/API.md) y en `GET /api/agent/tools`.
- **Grupos de herramientas**:

| Área | Herramientas |
| --- | --- |
| Resumen | `kafka_overview`, `kafka_status` |
| Plazos | `deadlines_list`, `deadline_get`, `deadline_add`, `deadlines_from_minutes`, `deadline_update`, `deadline_update_by_key`, `deadline_explain`, `deadline_delete` |
| Documentos | `docs_list`, `doc_get`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `doc_compare`, `extract_preview`, `document_link_tx` |
| Dosieres | `kafka_dossier`, `kafka_dossier_status`, `kafka_dossier_refresh`, `kafka_dossier_delete`, `kafka_requirements_diff` |
| Series y garantías | `series_list`, `price_history`, `warranty_check`, `phileas_sync` |
| Correo y carpetas | `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan` |
| Avisos y ajustes | `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run` |
| Taller | operaciones `pdf_*` (más de un centenar), `images_compress`, `tax_pack` |
| Lectura y OCR | `doc_extract`, `ocr_image`, `ocr_file`, `ocr_engine`, `ocr_pdf`, `ocr_status`, `doc_structure` |

- **Responsabilidad de los agentes**: toda escritura que un asistente hace por MCP necesita un `reason` de 3 a 300 caracteres y queda registrada en `data/agent_journal.jsonl` con agente, sesión y lo necesario para deshacerla. `POST /api/agent/undo` puede revertir una sesión entera (primero en simulación); algunas herramientas no se pueden deshacer y se indican como tales, y los cambios que haya hecho otra persona después se notifican como conflictos, no se pisan. Los tokens con perfil (`read_only`, `drafts`, `all`) se generan con `python -m hoard_link.tokens mint`.
- **Hub**: avisos (con respaldo en los canales propios de la app), pasarela de correo, agenda familiar (`GET /api/family/agenda`: renovaciones, mantenimiento y plazos de otras apps) y los eventos `kafka.document.archived`, `kafka.warranty.created`, `kafka.deadline.created`, `kafka.deadline.soon` y `kafka.deadline.overdue`. Otras apps llegan a Kafka por el hub (`POST /api/apps/kafka/call`) y usan `/api/ui/call`, `/api/agent/call`, `/api/dashboard` y `/api/documents/*`.
- **Copiloto**: un botón flotante de Faustus abre el asistente en un panel lateral. Se le informa del documento o plazo abierto (solo campos de resumen, nunca el texto del documento), de la página en que estás y de los campos de formulario sin guardar; la vista se recarga en el sitio tras cada turno y los formularios envían solo los campos que cambiaste.
- **App Dock**: junto a un chat de Faustus, la página registra `kafka_open`, `kafka_filter` y `kafka_view` para que el asistente abra documentos, aplique filtros y cambie de sección.
- **Skills**: `paperwork-intake-and-deadlines` y `hoard-daily-digest` se ofrecen a los agentes que se conectan.

## Estructura del proyecto

| Ruta | Contenido |
| --- | --- |
| `kafka_hoard/` | App FastAPI, almacén SQLite, reglas de extracción, calendario de días hábiles, planificador, avisos, correo, OCR, taller, herramientas del agente |
| `kafka_hoard/static/` | Interfaz React 19 + Vite + Tailwind ya compilada (código en `client/`) |
| `native/pdf/` | Código del motor PDF nativo |
| `mcp_server.py` | Puente MCP por stdio |
| `docs/` | Referencia de la API, arquitectura, reglas de plazos, dosieres, comparación de documentos, prueba comparativa de OCR |
| `tests/` | Suite de pytest |

```bash
python -m pytest -q    # sin red; los documentos y PDF se generan con datos inventados
```

Véase [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Límites

- El taller no lee páginas escaneadas (eso es `doc_extract` y `ocr_pdf`). Comprimir sin Ghostscript solo reduce imágenes; los archivos cifrados necesitan su contraseña; HEIC necesita el opcional `pillow-heif`; la conversión de Word y LibreOffice necesita tener uno de los dos instalado; la marca de agua es texto latino sencillo.
- Las fechas se leen con reglas para texto en español e inglés; un formato poco habitual puede acabar en la lista de revisión en lugar de convertirse en plazo.
- La pasada opcional del modelo solo propone datos, y se descarta todo aquello cuya fecha no esté escrita en el documento. Los plazos calculados por reglas no son asesoramiento legal: comprueba el aviso original en cualquier asunto importante.

## Licencia

MIT
