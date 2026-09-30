"""DVB-I discovery, FTA playback metadata, bouquets, picons and EPG."""

from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor, as_completed as asCompleted
from copy import copy
from datetime import datetime, time as datetimeTime, timezone
from errno import ENOSYS, EOPNOTSUPP, EPERM, EXDEV
from gzip import GzipFile
from hashlib import sha1, sha256
from io import BytesIO
from ipaddress import ip_address as ipAddress
from json import dump as jsonDump, dumps as jsonDumps, load as jsonLoad, loads as jsonLoads
from math import isfinite
from os import O_DIRECTORY, O_RDONLY, W_OK, X_OK, access, close, fdopen, fstat, fsync, link, listdir, makedirs, open as openFile, remove, replace, stat as fileStat, symlink, unlink
from os.path import abspath, basename, dirname, exists as pathExists, getmtime, getsize, isdir, isfile, join, lexists, normcase, normpath, realpath, samestat
from re import IGNORECASE, compile as reCompile, escape as reEscape, fullmatch as reFullmatch, match as reMatch, search as reSearch, split as reSplit, sub as reSub
from socket import timeout as socketTimeout
from struct import unpack
from tempfile import mkstemp
from time import localtime, strftime, time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl as parseQsl, quote, unquote, unquote_plus as unquotePlus, urlencode, urljoin, urlparse, urlsplit, urlunparse, urlunsplit
from urllib.request import Request, urlopen
from uuid import UUID
from warnings import catch_warnings as catchWarnings, simplefilter
from xml.etree.ElementTree import ParseError, fromstring, tostring
from xml.sax.saxutils import escape
from zlib import crc32

from . import _


# HTTP cache and atomic storage


def ensureDirectory(path):
	if path and not isdir(path):
		makedirs(path, exist_ok=True)


def atomicWrite(path, data, binary=False):
	"""Write *data* beside *path* and atomically replace the destination."""
	directory = dirname(abspath(path))
	ensureDirectory(directory)
	prefix = ".{0}.".format(basename(path))
	fd, temporary = mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
	try:
		mode = "wb" if binary else "w"
		kwargs = {} if binary else {"encoding": "utf-8", "newline": "\n"}
		with fdopen(fd, mode, **kwargs) as handle:
			handle.write(data)
			handle.flush()
			fsync(handle.fileno())
		replace(temporary, path)
		flags = O_DIRECTORY | O_RDONLY
		try:
			descriptor = openFile(directory, flags)
			try:
				fsync(descriptor)
			finally:
				close(descriptor)
		except Exception:
			# Some filesystems do not support syncing a directory descriptor.
			# The file itself is already synced.
			pass
	except Exception:
		try:
			close(fd)
		except Exception:
			pass
		try:
			unlink(temporary)
		except Exception:
			pass
		raise


def atomicWriteJson(path, value):
	atomicWrite(path, jsonDumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def readJson(path, default=None):
	try:
		with open(path, "r", encoding="utf-8") as handle:
			return jsonLoad(handle)
	except Exception:
		return {} if default is None else default


class FallbackStore:
	"""One flash-resident map, independent of the optional download cache."""

	def __init__(self, directory="/etc/enigma2"):
		self.path = join(directory, "dvbi_fallbacks.json")
		self.sources = {}

	def load(self):
		self.sources = {}
		if not isfile(self.path):
			return {}, {}
		# A damaged file must not be treated as an empty, overwritable map.
		with open(self.path, encoding="utf-8") as handle:
			stored = jsonLoad(handle)
		if not isinstance(stored, dict) or stored.get("version") != 1 or not isinstance(stored.get("automatic"), dict) or not isinstance(stored.get("manual"), dict):
			raise ValueError(_("Invalid fallback mapping file: {0}").format(self.path))
		self.sources = stored.get("sources", {})
		if not isinstance(self.sources, dict) or any(not isinstance(source, dict) for source in self.sources.values()):
			raise ValueError(_("Invalid fallback mapping file: {0}").format(self.path))
		return stored["automatic"], stored["manual"]

	@staticmethod
	def merge(lists, manual, preferAutomatic=True):
		automatic = {}
		conflicts = set()
		for services in lists.values():
			if not isinstance(services, list):
				raise ValueError(_("Invalid automatic fallback mappings"))
			for source, target in services:
				if source in automatic and automatic[source] != target:
					conflicts.add(source)
				automatic[source] = target
		# Conflicting service lists must not silently select another programme.
		for source in conflicts:
			automatic.pop(source)
		return (manual | automatic) if preferAutomatic else (automatic | manual)


class FetchResult:
	"""HTTP fetch result."""

	def __init__(self, url, content, changed, status, headers, cacheFile, stale=False):
		self.url = url
		self.content = content
		self.changed = changed
		self.status = status
		self.headers = headers
		self.cacheFile = cacheFile
		self.stale = bool(stale)


class ServiceListFetcher:
	"""Download DVB-I service lists with If-Modified-Since/ETag support."""

	USER_AGENT = "OpenATV-DvbIManager/0.3.6"
	MAX_WIRE_BYTES = 16 * 1024 * 1024
	MAX_CONTENT_BYTES = 32 * 1024 * 1024
	MAX_CACHE_AGE = 24 * 60 * 60
	DEFAULT_CACHE_QUOTA = 128 * 1024 * 1024

	def __init__(self, cacheDir, maxCacheBytes=DEFAULT_CACHE_QUOTA):
		self.cacheDir = cacheDir
		self.maxCacheBytes = max(8 * 1024 * 1024, min(int(maxCacheBytes), 1024 * 1024 * 1024))
		if not pathExists(self.cacheDir):
			makedirs(self.cacheDir)

	def cacheKey(self, url):
		return sha1(url.encode("utf-8")).hexdigest()

	def cachePaths(self, url):
		key = self.cacheKey(url)
		return (
			join(self.cacheDir, key + ".xml"),
			join(self.cacheDir, key + ".headers.json"),
		)

	def goodCachePath(self, url):
		return join(self.cacheDir, self.cacheKey(url) + ".good.xml")

	def markGood(self, url, content):
		"""Publish XML only after the caller parsed and accepted it."""
		if len(content) > self.MAX_CONTENT_BYTES:
			raise ValueError(_("DVB-I response exceeds the size limit"))
		target = self.goodCachePath(url)
		self.pruneCache(len(content), protected={target})
		atomicWrite(target, content, binary=True)

	def readLastGood(self, url):
		path = self.goodCachePath(url)
		if not pathExists(path):
			return b""
		with open(path, "rb") as handle:
			content = handle.read(self.MAX_CONTENT_BYTES + 1)
		if len(content) > self.MAX_CONTENT_BYTES:
			return b""
		return content

	def pruneCache(self, incomingBytes=0, protected=None):
		"""Keep receiver storage bounded; prefer retaining last-known-good XML."""
		if incomingBytes > self.maxCacheBytes:
			raise ValueError(_("DVB-I cache object exceeds the total cache quota"))
		protected = {abspath(path) for path in (protected or set())}
		files = []
		total = 0
		replaced = 0
		try:
			names = listdir(self.cacheDir)
		except OSError:
			return
		for name in names:
			if not name.endswith((".xml", ".good.xml", ".headers.json")):
				continue
			path = join(self.cacheDir, name)
			try:
				stat = fileStat(path)
			except OSError:
				continue
			total += stat.st_size
			if abspath(path) in protected:
				replaced += stat.st_size
			else:
				files.append((name.endswith(".good.xml"), stat.st_mtime, path, stat.st_size))
		total -= replaced
		files.sort(key=lambda item: (item[0], item[1]))
		for unusedIsGood, unusedMtime, path, size in files:
			if total + incomingBytes <= self.maxCacheBytes:
				break
			try:
				unlink(path)
				total -= size
			except OSError:
				pass
		if total + incomingBytes > self.maxCacheBytes:
			raise ValueError(_("DVB-I HTTP cache quota is exhausted"))

	def validateUrl(self, url):
		parsed = urlsplit(url or "")
		if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
			raise ValueError(_("DVB-I URL must use HTTP or HTTPS"))
		if parsed.username is not None or parsed.password is not None:
			raise ValueError(_("credentials in DVB-I URLs are not supported"))

	def cachedResult(self, url, cacheFile, headers, status, stale=False):
		with open(cacheFile, "rb") as handle:
			content = handle.read(self.MAX_CONTENT_BYTES + 1)
		if len(content) > self.MAX_CONTENT_BYTES:
			raise ValueError(_("cached DVB-I response exceeds the size limit"))
		return FetchResult(url, content, False, status, headers, cacheFile, stale=stale)

	def fetch(self, url, timeout=20, force=False):
		"""Fetch URL and return cached content on HTTP 304 or temporary network failure."""

		def cacheIsFresh(cachedHeaders):
			try:
				cachedAt = int(cachedHeaders.get("cached_at", "0"))
			except Exception:
				return False
			cacheControl = cachedHeaders.get("Cache-Control") or cachedHeaders.get("cache-control") or ""
			match = reSearch(r"(?:^|,)\s*max-age\s*=\s*(\d+)", cacheControl, IGNORECASE)
			if not match or "no-cache" in cacheControl.lower() or "no-store" in cacheControl.lower():
				return False
			lifetime = min(int(match.group(1)), self.MAX_CACHE_AGE)
			return lifetime > 0 and (int(time()) - cachedAt) < lifetime

		def readLimited(response, maximum):
			length = response.headers.get("Content-Length")
			if length:
				try:
					if int(length) > maximum:
						raise ValueError(_("DVB-I response exceeds the size limit"))
				except ValueError as error:
					if "size limit" in str(error):
						raise
			content = response.read(maximum + 1)
			if len(content) > maximum:
				raise ValueError(_("DVB-I response exceeds the size limit"))
			return content

		self.validateUrl(url)
		cacheFile, headerFile = self.cachePaths(url)
		try:
			with open(headerFile, "r", encoding="utf-8") as handle:
				cachedHeaders = jsonLoad(handle)
		except Exception:
			cachedHeaders = {}

		if not force and pathExists(cacheFile) and cacheIsFresh(cachedHeaders):
			return self.cachedResult(url, cacheFile, cachedHeaders, 304)

		request = Request(url)
		request.add_header("User-Agent", self.USER_AGENT)
		request.add_header("Accept", "application/xml,text/xml,*/*")
		request.add_header("Accept-Encoding", "gzip")

		if not force:
			etag = cachedHeaders.get("ETag") or cachedHeaders.get("etag")
			lastModified = cachedHeaders.get("Last-Modified") or cachedHeaders.get("last-modified")
			if etag:
				request.add_header("If-None-Match", etag)
			if lastModified:
				request.add_header("If-Modified-Since", lastModified)

		try:
			response = urlopen(request, timeout=timeout)
			try:
				status = getattr(response, "status", 200)
				headers = dict(response.info())
				self.validateUrl(response.geturl())
				content = readLimited(response, self.MAX_WIRE_BYTES)
			finally:
				response.close()

			encoding = ""
			for key, value in headers.items():
				if key.lower() == "content-encoding":
					encoding = value.lower()

			if "gzip" in encoding:
				content = GzipFile(fileobj=BytesIO(content)).read(self.MAX_CONTENT_BYTES + 1)
				if len(content) > self.MAX_CONTENT_BYTES:
					raise ValueError(_("uncompressed DVB-I response exceeds the size limit"))
			elif len(content) > self.MAX_CONTENT_BYTES:
				raise ValueError(_("DVB-I response exceeds the size limit"))

			self.pruneCache(len(content), protected={cacheFile})
			atomicWrite(cacheFile, content, binary=True)
			cachedHeaders = {str(key): str(value) for key, value in headers.items()}
			cachedHeaders["cached_at"] = str(int(time()))
			atomicWriteJson(headerFile, cachedHeaders)

			return FetchResult(url, content, True, status, headers, cacheFile)

		except HTTPError as err:
			if err.code == 304 and pathExists(cacheFile):
				return self.cachedResult(url, cacheFile, cachedHeaders, 304)
			if err.code in (408, 429, 500, 502, 503, 504) and pathExists(cacheFile):
				return self.cachedResult(url, cacheFile, cachedHeaders, err.code, stale=True)
			raise
		except URLError:
			if pathExists(cacheFile):
				return self.cachedResult(url, cacheFile, cachedHeaders, 0, stale=True)
			raise


# Service-list data and XML discovery


class DvbIServiceInstance:
	"""A single DVB-I service delivery instance."""

	def __init__(self):
		self.id = ""
		self.instanceId = ""
		self.priority = 0
		self.displayName = ""
		self.displayNames = []
		self.altServiceNames = []
		self.instanceType = "unknown"
		self.deliverySystem = "unknown"
		self.deliveryParameters = []
		self.sourceType = ""
		self.url = ""
		self.identifier = ""
		self.identifierScheme = ""
		self.contentType = ""
		# Worker-side bounded probe results.  Declared DVB-I metadata stays in
		# contentType/instanceType; detection is kept separate for auditing.
		self.detectedContentType = ""
		self.detectedMediaKind = ""
		self.mediaProbeStatus = ""
		self.mediaProbeEvidence = ""
		self.manifestVod = False
		self.serviceId = None
		self.transportStreamId = None
		self.originalNetworkId = None
		self.networkId = None
		self.namespace = None
		self.orbitalPosition = None
		self.frequency = None
		self.polarization = None
		self.drm = False
		self.drmSystemIds = []
		self.caSystemIds = []
		self.contentProtection = []
		self.hbbtv = False
		self.linkedApplications = []
		# None means that no Availability element was signalled.  A list
		# contains the periods exactly as supplied by the service list; the
		# time-dependent decision whether an instance is currently available
		# belongs in the instance selector, not in the XML parser.
		self.availability = None
		self.raw = {}


class DvbIService:
	"""A DVB-I service with one or more delivery instances."""

	def __init__(self):
		self.dvbiId = ""
		self.name = ""
		self.provider = ""
		self.country = ""
		self.language = ""
		# DVB ServiceTypeCS is the authoritative TV/radio discriminator.
		self.serviceTypeUri = ""
		self.serviceTypeTerm = ""
		self.mediaKind = "tv"
		self.mediaKindSource = "default_tv"
		self.regions = []
		self.lcn = None
		self.lcnSelectable = True
		self.lcnVisible = True
		self.lcnTableRegions = []
		self.logoUrls = []
		self.linkedApplications = []
		# Keep provider selection and the service-specific EPG identifier separate.
		self.contentGuideSourceRef = ""
		self.contentGuideSourceRefs = []
		self.contentGuideServiceRef = ""
		self.epgChannelId = ""
		self.instances = []
		self.flags = []
		self.matchedRef = ""
		self.matchedInstance = None
		self.selectedRef = ""
		self.selectedInstance = None
		self.selectedInstanceType = ""
		self.selectedPlayerType = ""
		self.selectedMediaKind = ""
		self.playbackToken = ""
		self.status = "not_checked"


class DvbIServiceList:
	"""A parsed DVB-I service list."""

	def __init__(self):
		self.name = "DVB-I"
		self.provider = ""
		self.listId = ""
		self.version = None
		self.responseStatus = ""
		self.schemaNamespace = ""
		self.sourceUrl = ""
		self.country = ""
		self.language = ""
		self.region = ""
		self.services = []
		self.contentGuideSources = {}
		self.defaultContentGuideSourceRef = ""
		# JSON-friendly dictionaries preserve every received table as well as
		# the table(s) selected for the requested region.
		self.lcnTables = []
		# Flat, hierarchy-aware catalogue parsed from RegionList.  Regions are
		# scoped to this service list and are never treated as SLR facets.
		self.regionCatalog = []
		self.regionCatalogVersion = None
		self.regionCatalogLanguage = ""
		self.raw = {}


def localName(tag):
	"""Return the local XML element or attribute name."""
	if tag is None:
		return ""
	if "}" in tag:
		return tag.rsplit("}", 1)[1]
	if ":" in tag:
		return tag.rsplit(":", 1)[1]
	return tag


def normalizeName(name):
	"""Normalize XML names for loose comparisons."""
	return localName(name).replace("_", "").replace("-", "").lower()


def attrValue(element, names, default=None):
	"""Return the first matching attribute value, matched by local name."""
	wanted = {normalizeName(name) for name in names}
	for key, value in element.attrib.items():
		if normalizeName(key) in wanted:
			return value
	return default


def textValue(element, default=""):
	"""Return stripped text for an element."""
	if element is None or element.text is None:
		return default
	return element.text.strip()


def iterChildren(element, names=None):
	"""Yield direct children, optionally filtered by local names."""
	if names is None:
		yield from list(element)
		return

	wanted = {normalizeName(name) for name in names}
	for child in list(element):
		if normalizeName(child.tag) in wanted:
			yield child


def iterDescendants(element, names=None):
	"""Yield descendants, optionally filtered by local names."""
	if names is None:
		for child in element.iter():
			if child is not element:
				yield child
		return

	wanted = {normalizeName(name) for name in names}
	for child in element.iter():
		if child is element:
			continue
		if normalizeName(child.tag) in wanted:
			yield child


def firstChild(element, names):
	"""Return the first direct child matching one of the local names."""
	for child in iterChildren(element, names):
		return child
	return None


def firstDescendant(element, names):
	"""Return the first descendant matching one of the local names."""
	for child in iterDescendants(element, names):
		return child
	return None


def firstText(element, names, default=""):
	"""Return the text of the first matching descendant."""
	child = firstDescendant(element, names)
	return textValue(child, default)


def allTexts(element, names):
	"""Return all non-empty descendant texts for matching local names."""
	result = []
	for child in iterDescendants(element, names):
		value = textValue(child, "")
		if value:
			result.append(value)
	return result


def childTextByLanguage(element, names, language=None):
	"""Pick text by xml:lang/lang if available, otherwise return the first text."""
	wanted = {normalizeName(name) for name in names}
	fallback = ""
	for child in element.iter():
		if normalizeName(child.tag) not in wanted:
			continue
		value = textValue(child, "")
		if not value:
			continue
		if not fallback:
			fallback = value
		if language:
			lang = attrValue(child, ["lang", "xml:lang", "{http://www.w3.org/XML/1998/namespace}lang"], "")
			if lang and lang.lower().split("-")[0] == language.lower().split("-")[0]:
				return value
	return fallback


def parseDateTime(value):
	value = (value or "").strip()
	if not value:
		return None
	if value.endswith(("Z", "z")):
		value = value[:-1] + "+00:00"
	parsed = datetime.fromisoformat(value)
	if parsed.tzinfo is None:
		parsed = parsed.replace(tzinfo=timezone.utc)
	return parsed


def parseClock(value, now):
	value = (value or "").strip()
	if not value:
		return None
	utc = value.endswith(("Z", "z"))
	if utc:
		value = value[:-1]
	parsed = datetimeTime.fromisoformat(value)
	reference = now.astimezone(timezone.utc) if utc else now
	return reference, parsed


def isInstanceAvailable(instance, now=None):
	def intervalAvailable(interval, now):
		try:
			startValue = parseClock(interval.get("start_time"), now)
			endValue = parseClock(interval.get("end_time"), now)
		except (TypeError, ValueError):
			return False
		if not startValue and not endValue:
			return True
		reference = (startValue or endValue)[0]
		current = reference.timetz().replace(tzinfo=None)
		start = startValue[1] if startValue else datetimeTime.min
		end = endValue[1] if endValue else datetimeTime.max
		day = reference.isoweekday()
		if current < end <= start:
			day = 7 if day == 1 else day - 1
		days = interval.get("days") or []
		if days and day not in days:
			return False
		if end > start:
			return start <= current < end
		# An end at/before start denotes an interval crossing midnight.
		return current >= start or current < end

	periods = getattr(instance, "availability", None)
	if periods is None:
		return True
	if not periods:
		return False
	now = now or datetime.now(timezone.utc)
	if now.tzinfo is None:
		now = now.replace(tzinfo=timezone.utc)
	for period in periods:
		try:
			validFrom = parseDateTime(period.get("valid_from"))
			validTo = parseDateTime(period.get("valid_to"))
		except (TypeError, ValueError):
			continue
		if validFrom and now < validFrom.astimezone(now.tzinfo):
			continue
		if validTo and now > validTo.astimezone(now.tzinfo):
			continue
		intervals = period.get("intervals") or []
		if not intervals or any(intervalAvailable(interval, now) for interval in intervals):
			return True
	return False


RADIO_SERVICE_TYPES = ("linear-radio", "ondemand-radio")
TV_SERVICE_TYPES = ("linear", "ondemand", "mosaic", "other")


def serviceTypeTerm(value):
	"""Return the normalized DVB ServiceTypeCS term from a URI or term."""
	value = str(value or "").strip().lower().rstrip("/#:")
	if not value:
		return ""
	for separator in ("#", "/", ":"):
		if separator in value:
			value = value.rsplit(separator, 1)[-1]
	return value.strip()


def classifyService(service):
	"""Return ``(tv|radio, evidence)`` and persist it on the model.

	DVB-I ServiceTypeCS is authoritative.  Fallbacks exist only for legacy or
	incomplete lists and deliberately avoid service names and URL suffixes.
	"""

	def isAudioOnlyContentType(value):
		# Playlist/container MIME values such as audio/mpegurl do not prove that
		# an adaptive presentation contains no video.
		return value in (
			"audio/aac",
			"audio/aacp",
			"audio/flac",
			"audio/mpeg",
			"audio/mp4",
			"audio/ogg",
			"audio/wav",
			"audio/x-wav",
			"application/ogg",
		)

	def instanceContentTypes(instance):
		for attribute in ("contentType", "detectedContentType"):
			value = str(getattr(instance, attribute, "") or "")
			value = value.split(";", 1)[0].strip().lower()
			if value:
				yield value

	def referenceServiceType(reference):
		"""Read Enigma2 data[0] from a service reference (hex encoded)."""
		parts = str(reference or "").split(":")
		if len(parts) < 3 or not parts[2]:
			return None
		try:
			return int(parts[2], 16)
		except (TypeError, ValueError):
			return None

	uri = getattr(service, "serviceTypeUri", "") or getattr(service, "service_type", "")
	term = serviceTypeTerm(uri)
	if term in RADIO_SERVICE_TYPES:
		kind, source = "radio", "service_type:{0}".format(term)
	elif term in TV_SERVICE_TYPES:
		kind, source = "tv", "service_type:{0}".format(term)
	elif term == "data":
		kind, source = "data", "service_type:data"
	elif term:
		kind, source = "tv", "service_type_unknown:{0}".format(term)
	else:
		referenceType = referenceServiceType(getattr(service, "matchedRef", ""))
		if referenceType in (2, 10):
			kind, source = "radio", "broadcast_reference"
		else:
			hasVideo = False
			hasAudio = False
			hasRadioDelivery = False
			for instance in getattr(service, "instances", []):
				if str(getattr(instance, "instanceType", "") or "").lower() == "radio":
					hasRadioDelivery = True
				for contentType in instanceContentTypes(instance):
					hasVideo = hasVideo or contentType.startswith("video/")
					hasAudio = hasAudio or isAudioOnlyContentType(contentType)
			if hasVideo:
				kind, source = "tv", "video_content_type"
			elif hasRadioDelivery:
				kind, source = "radio", "radio_delivery"
			elif hasAudio:
				kind, source = "radio", "audio_content_type"
			else:
				kind, source = "tv", "default_tv"

	service.serviceTypeTerm = term
	service.mediaKind = kind
	service.mediaKindSource = source
	return kind, source


REGION_QUERY_PARAMETER = "regionID"


def baseServiceListUrl(url):
	"""Remove only regionID while preserving opaque/signed query bytes."""
	parsed = urlsplit(str(url or "").strip())
	queryParts = []
	for part in parsed.query.split("&") if parsed.query else []:
		key = part.split("=", 1)[0]
		if unquotePlus(key) != REGION_QUERY_PARAMETER:
			queryParts.append(part)
	return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "&".join(queryParts), parsed.fragment))


def withRegionId(url, regionId):
	parsed = urlsplit(baseServiceListUrl(url))
	query = parsed.query
	if regionId:
		value = REGION_QUERY_PARAMETER + "=" + quote(str(regionId), safe="")
		query = query + "&" + value if query else value
	return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def regionCatalogKey(url):
	parsed = urlsplit(baseServiceListUrl(url))
	identity = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))
	return sha256(identity.encode("utf-8")).hexdigest()[:24]


def regionCatalogPath(metadataDir, url):
	return join(metadataDir, "regions", regionCatalogKey(url) + ".json")


SERVICE_LOGO_PREFIX = "urn:dvb:metadata:cs:howrelatedcs:"
SERVICE_LOGO_SUFFIX = ":1001.2"
LINKED_APPLICATION_PREFIX = "urn:dvb:metadata:cs:linkedapplicationcs:"
LINKED_APPLICATION_PARALLEL_SUFFIX = ":1.1"
LINKED_APPLICATION_CONTROLS_MEDIA_SUFFIX = ":1.2"
ICECAST_PREFIX = "urn:dvb:icecast:v1:"


def safeInt(value):
	if value is None:
		return None
	if isinstance(value, int):
		return value
	text = str(value).strip()
	if not text:
		return None
	try:
		if text.lower().startswith("0x"):
			return int(text, 16)
		if reMatch(r"^[0-9a-fA-F]+$", text) and any(c in text.lower() for c in "abcdef"):
			return int(text, 16)
		return int(text, 10)
	except (TypeError, ValueError):
		return None


def safeBool(value, default=False):
	if value is None:
		return default
	normalized = str(value).strip().lower()
	if normalized in ("true", "1", "yes"):
		return True
	if normalized in ("false", "0", "no"):
		return False
	return default


def directChildren(element, names=None):
	if names is None:
		return list(element)
	wanted = set(normalizeName(name) for name in names)
	return [child for child in list(element) if normalizeName(child.tag) in wanted]


def firstDirectChild(element, names):
	children = directChildren(element, names)
	return children[0] if children else None


def findFirstDescendant(element, names):
	for child in iterDescendants(element, names):
		return child
	return None


def directTexts(element, names):
	result = []
	for child in directChildren(element, names):
		value = textValue(child, "")
		if value:
			result.append(value)
	return result


def firstDirectText(element, names, default=""):
	values = directTexts(element, names)
	return values[0] if values else default


def directTextByLanguage(element, names, language=None):
	fallback = ""
	for child in directChildren(element, names):
		value = textValue(child, "")
		if not value:
			continue
		if not fallback:
			fallback = value
		if language:
			lang = attrValue(child, ["lang", "xml:lang"], "")
			if lang and lang.lower().split("-", 1)[0] == language.lower().split("-", 1)[0]:
				return value
	return fallback


def firstInt(element, names):
	wanted = set(normalizeName(name) for name in names)
	for child in element.iter():
		if normalizeName(child.tag) in wanted:
			parsed = safeInt(textValue(child, ""))
			if parsed is not None:
				return parsed
	for name in names:
		parsed = safeInt(attrValue(element, [name], None))
		if parsed is not None:
			return parsed
	return None


def looksLikeHttpUrl(value):
	value = (value or "").strip().lower()
	return value.startswith("http://") or value.startswith("https://")


def looksLikeStreamUrl(value):
	value = (value or "").strip().lower()
	return value.startswith(("http://", "https://", "rtsp://", "rtp://", "udp://"))


def firstValueOrAttr(element, names):
	"""Return an endpoint, retaining non-HTTP values used by local fixtures."""

	def firstUrlInElement(element):
		"""Return the first HTTP(S) endpoint in an endpoint-like element."""
		if element is None:
			return ""

		for child in element.iter():
			value = textValue(child, "")
			if looksLikeHttpUrl(value):
				return value
			for attrName in ("href", "uri", "url", "endpoint", "src", "template", "templateUrl", "templateUri"):
				value = attrValue(child, [attrName], "")
				if looksLikeHttpUrl(value):
					return value
		return ""

	for name in names:
		for child in iterDescendants(element, [name]):
			value = firstUrlInElement(child)
			if value:
				return value
			value = textValue(child, "")
			if value:
				return value
			for descendant in child.iter():
				value = textValue(descendant, "")
				if value:
					return value

	for name in names:
		value = attrValue(element, [name], "")
		if value:
			return value
	return ""


def appendUniqueCasefold(values, value):
	if not value:
		return
	normalized = value.casefold()
	if all(existing.casefold() != normalized for existing in values):
		values.append(value)


def uniquePreserve(values):
	result = []
	for value in values:
		if value and value not in result:
			result.append(value)
	return result


class ServiceListParser:
	"""Parse DVB-I service list XML into receiver-friendly models."""

	def parse(self, content, sourceUrl="", country="", language="", region=""):
		# Passing bytes directly is important: ElementTree then honours the XML
		# declaration (including UTF-16) instead of corrupting it through an
		# unconditional UTF-8 decode/re-encode cycle.
		def parseServices(root, country, language, contentGuideSources, defaultContentGuideSourceRef, lcnEntries):
			def parseService(element, country, language, contentGuideSources, defaultContentGuideSourceRef, lcnEntries):
				def parseInstance(element, language):
					def instanceUrl(deliveryNodes):
						locatorNames = set(
							normalizeName(name)
							for name in (
								"URI",
								"URL",
								"Url",
								"RTSPURL",
								"StreamURL",
								"MediaPresentationDescriptionUri",
								"MediaPresentationDescriptionURL",
								"DASHURL",
								"HLSURL",
								"Locator",
								"UriBasedLocation",
							)
						)
						for delivery in deliveryNodes:
							for child in delivery.iter():
								if normalizeName(child.tag) not in locatorNames:
									continue
								value = textValue(child, "")
								if looksLikeStreamUrl(value):
									return value
								for attrName in ("href", "uri", "url", "src"):
									value = attrValue(child, [attrName], "")
									if looksLikeStreamUrl(value):
										return value

						multicast = next(
							(child for child in deliveryNodes if normalizeName(child.tag) == "multicasttsdeliveryparameters"),
							None,
						)
						if multicast is not None:
							addressElement = findFirstDescendant(multicast, ["IPMulticastAddress"])
							if addressElement is not None:
								address = attrValue(addressElement, ["Address"], "")
								port = attrValue(addressElement, ["Port"], "")
								streaming = attrValue(addressElement, ["Streaming"], "udp").lower()
								if address and port and streaming in ("udp", "rtp"):
									return "{0}://{1}:{2}".format(streaming, address, port)
						return ""

					def decodeIdentifier(identifier):
						value = (identifier or "").strip()
						lower = value.lower()
						if lower.startswith(ICECAST_PREFIX):
							decoded = unquote(value[len(ICECAST_PREFIX):])
							return (decoded if looksLikeHttpUrl(decoded) else "", ICECAST_PREFIX[:-1], "icecast")
						if looksLikeStreamUrl(value):
							return value, "url", "identifier"
						if lower.startswith("urn:"):
							parts = value.split(":")
							scheme = ":".join(parts[:4]) if len(parts) >= 4 else value
							return "", scheme, "identifier"
						return "", "", "identifier"

					def instanceContentType(element, deliveryNodes):
						identifier = firstDirectChild(element, ["IdentifierBasedDeliveryParameters"])
						if identifier is not None:
							value = attrValue(identifier, ["contentType"], "")
							if value:
								return value
						for delivery in deliveryNodes:
							for child in delivery.iter():
								value = attrValue(child, ["contentType"], "")
								if value:
									return value
						return ""

					def parseContentProtection(element, instance):
						protectionElements = directChildren(element, ["ContentProtection"])
						instance.drm = bool(protectionElements)
						for protection in protectionElements:
							for system in iterDescendants(protection, ["DRMSystemId", "CASystemId"]):
								systemId = textValue(system, "")
								if not systemId:
									continue
								isDrm = normalizeName(system.tag) == "drmsystemid"
								item = {
									"kind": "drm" if isDrm else "ca",
									"system_id": systemId,
									"cps_index": attrValue(system, ["cpsIndex"], ""),
									"encryption_scheme": attrValue(system, ["encryptionScheme"], ""),
									"la_url": attrValue(system, ["LAURL"], ""),
									"certificate_url": attrValue(system, ["certificateURL"], ""),
								}
								instance.contentProtection.append(item)
								if isDrm:
									appendUniqueCasefold(instance.drmSystemIds, systemId)
								else:
									appendUniqueCasefold(instance.caSystemIds, systemId)

					instance = DvbIServiceInstance()
					instance.id = attrValue(element, ["id"], "0")
					instance.instanceId = instance.id
					instance.priority = safeInt(attrValue(element, ["priority"], 0))
					if instance.priority is None:
						instance.priority = 0
					instance.displayNames = directTexts(element, ["DisplayName"])
					instance.displayName = directTextByLanguage(element, ["DisplayName"], language)
					instance.altServiceNames = directTexts(element, ["AltServiceName"])
					instance.sourceType = firstDirectText(element, ["SourceType"], "")
					instance.linkedApplications = self.linkedApplications(element)
					instance.hbbtv = bool(instance.linkedApplications)
					instance.availability = self.availability(element)

					parseContentProtection(element, instance)

					deliveryNodes = [child for child in directChildren(element) if normalizeName(child.tag).endswith("deliveryparameters")]
					instance.deliveryParameters = [localName(child.tag) for child in deliveryNodes]
					instance.contentType = instanceContentType(element, deliveryNodes)
					instance.identifier = firstDirectText(element, ["IdentifierBasedDeliveryParameters"], "")
					identifierUrl, identifierScheme, identifierSystem = decodeIdentifier(instance.identifier)
					instance.identifierScheme = identifierScheme

					instance.instanceType, instance.deliverySystem = self.instanceType(
						deliveryNodes,
						instance.contentType,
						instance.identifier,
						identifierSystem,
						instance.sourceType,
						instance.linkedApplications,
					)
					instance.url = identifierUrl or instanceUrl(deliveryNodes)

					triplet = findFirstDescendant(element, ["DVBTriplet"])
					if triplet is not None:
						instance.serviceId = safeInt(attrValue(triplet, ["serviceId", "sid"], None))
						instance.transportStreamId = safeInt(attrValue(triplet, ["tsId", "transportStreamId"], None))
						instance.originalNetworkId = safeInt(attrValue(triplet, ["origNetId", "originalNetworkId"], None))
						# Retain compatibility with early/non-conforming lists that used
						# child elements rather than the standard DVBTriplet attributes.
						if instance.serviceId is None:
							instance.serviceId = firstInt(triplet, ["ServiceId", "SID", "DVBServiceId"])
						if instance.transportStreamId is None:
							instance.transportStreamId = firstInt(triplet, ["TransportStreamId", "TSID", "DVBTransportStreamId"])
						if instance.originalNetworkId is None:
							instance.originalNetworkId = firstInt(triplet, ["OriginalNetworkId", "ONID", "DVBOriginalNetworkId"])
					else:
						instance.serviceId = firstInt(element, ["ServiceId", "SID", "DVBServiceId"])
						instance.transportStreamId = firstInt(element, ["TransportStreamId", "TSID", "DVBTransportStreamId"])
						instance.originalNetworkId = firstInt(element, ["OriginalNetworkId", "ONID", "DVBOriginalNetworkId"])

					instance.networkId = firstInt(element, ["NetworkId", "NID"])
					instance.namespace = firstInt(element, ["Namespace", "DVBNamespace"])
					instance.orbitalPosition = self.firstDescendantText(element, ["OrbitalPosition", "SatellitePosition"])
					instance.frequency = self.firstDescendantText(element, ["Frequency", "CentreFrequency"])
					instance.polarization = self.firstDescendantText(element, ["Polarization", "Polarisation"])
					instance.raw = {
						"id": instance.id,
						"priority": instance.priority,
						"type": instance.instanceType,
						"delivery_system": instance.deliverySystem,
						"delivery_parameters": list(instance.deliveryParameters),
						"url": instance.url,
						"identifier": instance.identifier,
						"content_type": instance.contentType,
					}
					return instance

				def inlineLcn(element):
					for child in directChildren(element, ["LCN", "LogicalChannelNumber", "ChannelNumber"]):
						value = safeInt(textValue(child, ""))
						if value is None:
							value = safeInt(attrValue(child, ["channelNumber", "value"], None))
						if value is not None:
							return value
					return None

				def serviceIdentifier(element):
					value = firstDirectText(element, ["UniqueIdentifier", "DvbIServiceId", "ServiceIdentifier", "GlobalServiceId"], "")
					if value:
						return value
					return attrValue(element, ["serviceId", "id", "sid"], "")

				service = DvbIService()
				service.name = directTextByLanguage(element, ["ServiceName", "Name", "Title"], language)
				service.provider = directTextByLanguage(element, ["ProviderName", "Provider", "ServiceProvider"], language)
				service.dvbiId = serviceIdentifier(element)
				service.country = country
				service.language = language
				serviceType = firstDirectChild(element, ["ServiceType"])
				if serviceType is not None:
					service.serviceTypeUri = attrValue(serviceType, ["href", "uri"], "")
					if not service.serviceTypeUri:
						service.serviceTypeUri = textValue(serviceType, "")
				service.regions = self.regions(element)
				service.logoUrls = self.logos(element)
				service.linkedApplications = self.linkedApplications(element)

				inlineSource = firstDirectChild(element, ["ContentGuideSource"])
				if inlineSource is not None:
					sourceRef = self.sourceIdentifier(inlineSource)
				else:
					sourceRef = firstDirectText(
						element,
						[
							"ContentGuideSourceRef",
							"ContentGuideSourceReference",
							"ContentGuideSourceId",
							"ContentGuideSourceID",
							"ScheduleSourceRef",
							"CGSIDRef",
						],
						"",
					)
					if not sourceRef:
						sourceRef = attrValue(
							element,
							["CGSIDRef", "cgsidRef", "contentGuideSourceRef", "contentGuideSourceId"],
							"",
						)
				if not sourceRef and defaultContentGuideSourceRef:
					sourceRef = defaultContentGuideSourceRef
				if sourceRef and sourceRef not in contentGuideSources and len(contentGuideSources) == 1:
					# Retain the explicit value even when a producer forgot the matching
					# source declaration; consumers can report the unresolved mapping.
					pass

				service.contentGuideSourceRef = sourceRef
				service.contentGuideSourceRefs = [sourceRef] if sourceRef else []
				service.contentGuideServiceRef = firstDirectText(element, ["ContentGuideServiceRef"], "")

				for instanceElement in directChildren(element, ["ServiceInstance"]):
					service.instances.append(parseInstance(instanceElement, language))

				if not service.dvbiId:
					seed = "|".join([country or "", service.provider or "", service.name or ""])
					service.dvbiId = "generated:" + sha1(seed.encode("utf-8")).hexdigest()

				lcnEntry = lcnEntries.get(service.dvbiId)
				if lcnEntry:
					service.lcn = lcnEntry["channel_number"]
					service.lcnSelectable = lcnEntry["selectable"]
					service.lcnVisible = lcnEntry["visible"]
					service.lcnTableRegions = list(lcnEntry["table_regions"])
				else:
					service.lcn = inlineLcn(element)

				service.epgChannelId = service.contentGuideServiceRef or service.dvbiId
				service.flags = self.flags(service)
				classifyService(service)
				return service

			def isProbableService(element):
				hasName = bool(directTextByLanguage(element, ["ServiceName", "Name", "Title"], None))
				hasId = bool(firstDirectText(element, ["UniqueIdentifier", "ServiceIdentifier", "DvbIServiceId", "ServiceId"], ""))
				hasInstance = bool(directChildren(element, ["ServiceInstance"]))
				return hasName or hasId or hasInstance

			serviceElements = directChildren(root, ["Service", "TestService"])
			if not serviceElements:
				serviceElements = list(iterDescendants(root, ["Service", "TestService"]))

			services = []
			seen = set()
			for element in serviceElements:
				if not isProbableService(element):
					continue
				service = parseService(
					element,
					country,
					language,
					contentGuideSources,
					defaultContentGuideSourceRef,
					lcnEntries,
				)
				if not service.name and not service.dvbiId:
					continue
				key = service.dvbiId or service.name
				if key in seen:
					continue
				seen.add(key)
				services.append(service)
			return services

		def selectLcnEntries(tables, region):
			def requestedRegions(region):
				if isinstance(region, (list, tuple, set)):
					values = region
				elif region:
					values = reSplit(r"[,;]", str(region))
				else:
					values = []
				return set(value.strip().casefold() for value in values if str(value).strip())

			requested = requestedRegions(region)
			matching = []
			defaults = []
			for index, table in enumerate(tables):
				tableRegions = set(value.casefold() for value in table["target_regions"])
				if not tableRegions:
					defaults.append(index)
				elif requested and tableRegions.intersection(requested):
					matching.append(index)

			if matching:
				selected = matching
			elif defaults:
				selected = defaults
			elif len(tables) == 1:
				# A single regional table is unambiguous and is preferable to
				# losing every LCN when a caller has not supplied a region yet.
				selected = [0]
			else:
				selected = []

			entries = {}
			for index in selected:
				table = tables[index]
				for entry in table["entries"]:
					if entry["service_ref"] not in entries:
						mapped = dict(entry)
						mapped["table_regions"] = list(table["target_regions"])
						mapped["table_index"] = index
						entries[entry["service_ref"]] = mapped
			return entries, selected

		def parseLcnTables(root):
			tables = []
			for element in iterDescendants(root, ["LCNTable"]):
				entries = []
				for lcn in directChildren(element, ["LCN"]):
					serviceRef = attrValue(lcn, ["serviceRef", "serviceReference", "ref"], "")
					channelNumber = safeInt(attrValue(lcn, ["channelNumber", "lcn"], None))
					if not serviceRef or channelNumber is None:
						continue
					entries.append(
						{
							"service_ref": serviceRef,
							"channel_number": channelNumber,
							"selectable": safeBool(attrValue(lcn, ["selectable"], None), True),
							"visible": safeBool(attrValue(lcn, ["visible"], None), True),
						}
					)

				targetRegions = directTexts(element, ["TargetRegion"])
				tables.append(
					{
						"version": safeInt(attrValue(element, ["version"], None)),
						"target_regions": targetRegions,
						"is_default": not bool(targetRegions),
						"preserve_broadcast_lcn": safeBool(attrValue(element, ["preserveBroadcastLCN"], None), False),
						"entries": entries,
						"selected": False,
					}
				)
			return tables

		def parseContentGuideSources(root):
			result = {}
			for element in iterDescendants(root, ["ContentGuideSource"]):
				sourceId = self.sourceIdentifier(element)
				result[sourceId] = {
					"id": sourceId,
					"name": directTextByLanguage(element, ["Name", "Title", "ContentGuideSourceName"], None),
					"provider": directTextByLanguage(element, ["ProviderName", "Provider", "ServiceProvider"], None),
					"schedule": firstValueOrAttr(element, ["ScheduleInfoEndpoint", "ScheduleEndpoint", "ScheduleInfoURL", "ScheduleInfoUrl"]),
					"program": firstValueOrAttr(element, ["ProgramInfoEndpoint", "ProgramEndpoint", "ProgramInfoURL", "ProgramInfoUrl"]),
					"group": firstValueOrAttr(element, ["GroupInfoEndpoint", "GroupEndpoint", "GroupInfoURL", "GroupInfoUrl"]),
					"more_episodes": firstValueOrAttr(element, ["MoreEpisodesEndpoint", "MoreEpisodesURL", "MoreEpisodesUrl"]),
				}
				result[sourceId]["has_schedule"] = bool(result[sourceId]["schedule"])
				result[sourceId]["has_program"] = bool(result[sourceId]["program"])
				result[sourceId]["has_group"] = bool(result[sourceId]["group"])
			return result

		if isinstance(content, bytearray):
			content = bytes(content)
		elif isinstance(content, memoryview):
			content = content.tobytes()
		if not isinstance(content, (bytes, str)):
			raise TypeError(_("DVB-I service list content must be bytes or text"))

		root = fromstring(content)
		schemaNamespace = ""
		if isinstance(root.tag, str) and root.tag.startswith("{") and "}" in root.tag:
			schemaNamespace = root.tag[1:].split("}", 1)[0]
		if normalizeName(root.tag) != "servicelist":
			raise ValueError(_("DVB-I service list root must be ServiceList"))
		if not schemaNamespace.startswith("urn:dvb:metadata:servicediscovery:"):
			raise ValueError(_("unsupported DVB-I service list namespace"))

		serviceList = DvbIServiceList()
		serviceList.sourceUrl = sourceUrl
		serviceList.country = country
		rootLanguage = attrValue(root, ["lang", "xml:lang"], "")
		serviceList.language = language or rootLanguage
		serviceList.region = region or ""
		serviceList.name = directTextByLanguage(root, ["Name", "ServiceListName", "ServiceListProviderName", "Title"], serviceList.language) or "DVB-I"
		serviceList.provider = directTextByLanguage(root, ["ProviderName", "Provider", "ServiceProvider"], serviceList.language)
		serviceList.listId = attrValue(root, ["id"], "")
		serviceList.version = safeInt(attrValue(root, ["version"], None))
		serviceList.responseStatus = attrValue(root, ["responseStatus"], "")
		serviceList.schemaNamespace = schemaNamespace

		serviceList.contentGuideSources = parseContentGuideSources(root)
		serviceList.defaultContentGuideSourceRef = self.defaultContentGuideSourceRef(root, serviceList.contentGuideSources)

		serviceList.lcnTables = parseLcnTables(root)
		(
			serviceList.regionCatalog,
			serviceList.regionCatalogVersion,
			serviceList.regionCatalogLanguage,
		) = self.parseRegionCatalog(root, serviceList.language)
		lcnEntries, selectedTableIndexes = selectLcnEntries(serviceList.lcnTables, region)
		for index, table in enumerate(serviceList.lcnTables):
			table["selected"] = index in selectedTableIndexes

		serviceList.services = parseServices(
			root,
			country,
			serviceList.language,
			serviceList.contentGuideSources,
			serviceList.defaultContentGuideSourceRef,
			lcnEntries,
		)
		serviceList.raw = {
			"root_attributes": dict(root.attrib),
			"selected_lcn_table_indexes": selectedTableIndexes,
		}
		return serviceList

	def sourceIdentifier(self, element):
		sourceId = attrValue(
			element,
			["CGSID", "cgsid", "id", "xml:id", "contentGuideSourceId", "sourceId"],
			"",
		)
		if not sourceId:
			sourceId = firstDirectText(element, ["CGSID", "ContentGuideSourceId", "ContentGuideSourceID", "Id", "ID"], "")
		if not sourceId:
			sourceId = "cgs-" + sha1(tostring(element, encoding="utf-8")).hexdigest()[:12]
		return sourceId

	def defaultContentGuideSourceRef(self, root, sources):
		directSource = firstDirectChild(root, ["ContentGuideSource"])
		if directSource is not None:
			return self.sourceIdentifier(directSource)
		if len(sources) == 1:
			return sorted(sources.keys())[0]
		return ""

	def parseRegionCatalog(self, root, language):
		"""Return a flat catalogue while preserving the RegionList hierarchy."""
		regionLists = directChildren(root, ["RegionList"])
		if not regionLists:
			regionLists = list(iterDescendants(root, ["RegionList"]))
		result = []
		version = None
		catalogLanguage = ""

		def values(element, childName, inheritedLanguage):
			items = []
			for child in directChildren(element, [childName]):
				value = textValue(child, "")
				if value:
					items.append(
						{
							"value": value,
							"language": attrValue(child, ["lang", "xml:lang"], "") or inheritedLanguage,
						}
					)
			return items

		def walk(element, parentId="", depth=0, parentPath="", inheritedCountryCodes=None, inheritedLanguage=""):
			regionId = attrValue(element, ["regionID", "regionId", "id"], "").strip()
			effectiveLanguage = attrValue(element, ["lang", "xml:lang"], "") or inheritedLanguage
			names = values(element, "RegionName", effectiveLanguage)
			name = directTextByLanguage(element, ["RegionName"], language) or regionId
			path = " / ".join(item for item in (parentPath, name) if item)
			declaredCountryCodes = [item for item in reSplit(r"[\s,;]+", attrValue(element, ["countryCodes", "countryCode"], "").strip()) if item]
			countryCodes = uniquePreserve(declaredCountryCodes or inheritedCountryCodes or [])
			if regionId:
				postcodeRanges = []
				for postcodeRange in directChildren(element, ["PostcodeRange"]):
					postcodeRanges.append(
						{
							"from": attrValue(postcodeRange, ["from"], ""),
							"to": attrValue(postcodeRange, ["to"], ""),
						}
					)
				coordinates = []
				for coordinate in directChildren(element, ["Coordinates"]):
					coordinates.append(
						{
							"latitude": firstDirectText(coordinate, ["Latitude"], ""),
							"longitude": firstDirectText(coordinate, ["Longitude"], ""),
							"radius": firstDirectText(coordinate, ["Radius"], ""),
						}
					)
				result.append(
					{
						"region_id": regionId,
						"name": name,
						"display_path": path or regionId,
						"names": names,
						"selectable": safeBool(attrValue(element, ["selectable"], None), True),
						"parent_id": parentId,
						"depth": depth,
						"country_codes": uniquePreserve(countryCodes),
						"postcodes": directTexts(element, ["Postcode"]),
						"wildcard_postcodes": directTexts(element, ["WildcardPostcode"]),
						"postcode_ranges": postcodeRanges,
						"coordinates": coordinates,
					}
				)
			nextParent = regionId or parentId
			nextPath = path or parentPath
			for child in directChildren(element, ["Region"]):
				walk(child, nextParent, depth + 1, nextPath, countryCodes, effectiveLanguage)

		for regionList in regionLists:
			if version is None:
				version = safeInt(attrValue(regionList, ["version"], None))
			listLanguage = attrValue(regionList, ["lang", "xml:lang"], "") or attrValue(root, ["lang", "xml:lang"], "")
			if not catalogLanguage:
				catalogLanguage = listLanguage
			for region in directChildren(regionList, ["Region"]):
				walk(region, inheritedLanguage=listLanguage)
		return result, version, catalogLanguage

	def regions(self, element):
		result = []
		for value in directTexts(element, ["TargetRegion", "Region", "ServiceRegion", "Postcode", "PostalCode"]):
			if value not in result:
				result.append(value)
		return result

	def relatedMedia(self, element):
		result = []
		for related in directChildren(element, ["RelatedMaterial"]):
			howRelated = findFirstDescendant(related, ["HowRelated"])
			href = attrValue(howRelated, ["href"], "") if howRelated is not None else ""
			for media in iterDescendants(related, ["MediaUri", "MediaURL", "MediaUrl"]):
				value = textValue(media, "")
				if not value:
					value = attrValue(media, ["href", "uri", "url", "src"], "")
				if value:
					result.append(
						{
							"how_related": href,
							"url": value,
							"content_type": attrValue(media, ["contentType", "type"], ""),
						}
					)
		return result

	def logos(self, element):
		def isServiceLogoRelation(href):
			value = (href or "").strip().lower()
			return value.startswith(SERVICE_LOGO_PREFIX) and value.endswith(SERVICE_LOGO_SUFFIX)

		result = []
		for media in self.relatedMedia(element):
			if not isServiceLogoRelation(media["how_related"]):
				continue
			if looksLikeHttpUrl(media["url"]) and media["url"] not in result:
				result.append(media["url"])
		return result

	def linkedApplications(self, element):
		def linkedApplicationRelation(href):
			value = (href or "").strip().lower()
			if not value.startswith(LINKED_APPLICATION_PREFIX):
				return ""
			if value.endswith(LINKED_APPLICATION_PARALLEL_SUFFIX):
				return "parallel"
			if value.endswith(LINKED_APPLICATION_CONTROLS_MEDIA_SUFFIX):
				return "controls_media"
			return "linked"

		result = []
		for media in self.relatedMedia(element):
			relation = linkedApplicationRelation(media["how_related"])
			if not relation:
				continue
			app = dict(media)
			app["relation"] = relation
			result.append(app)
		return result

	def firstDescendantText(self, element, names):
		child = findFirstDescendant(element, names)
		return textValue(child, "") if child is not None else ""

	def availability(self, element):
		availability = firstDirectChild(element, ["Availability"])
		if availability is None:
			return None
		periods = []
		for period in directChildren(availability, ["Period"]):
			intervals = []
			for interval in directChildren(period, ["Interval"]):
				daysText = attrValue(interval, ["days"], "1 2 3 4 5 6 7")
				days = []
				for value in reSplit(r"[\s,]+", daysText.strip()):
					parsed = safeInt(value)
					if parsed is not None:
						days.append(parsed)
				intervals.append(
					{
						"days": days,
						"days_raw": daysText,
						"recurrence": safeInt(attrValue(interval, ["recurrence"], 1)) or 1,
						"start_time": attrValue(interval, ["startTime"], "00:00:00Z"),
						"end_time": attrValue(interval, ["endTime"], "23:59:59.999Z"),
					}
				)
			periods.append(
				{
					"valid_from": attrValue(period, ["validFrom"], ""),
					"valid_to": attrValue(period, ["validTo"], ""),
					"intervals": intervals,
				}
			)
		return periods

	def instanceType(self, deliveryNodes, contentType, identifier, identifierSystem, sourceType, linkedApplications):
		names = set(normalizeName(child.tag) for child in deliveryNodes)
		contentTypeLower = (contentType or "").lower()
		sourceTypeLower = (sourceType or "").lower()

		if "dvbtdeliveryparameters" in names:
			return "dvb-t", "dvb-t"
		if "dvbsdeliveryparameters" in names:
			return "dvb-s", "dvb-s"
		if "dvbcdeliveryparameters" in names:
			return "dvb-c", "dvb-c"
		if "dashdeliveryparameters" in names:
			return "dash", "dash"
		if "rtspdeliveryparameters" in names:
			return "rtsp", "rtsp"
		if "multicasttsdeliveryparameters" in names:
			return "multicast", "multicast-ts"
		if identifierSystem == "icecast":
			# "radio" remains compatible with the current bouquet writer;
			# deliverySystem carries the precise DVB Icecast profile.
			return "radio", "icecast"
		if "mpegurl" in contentTypeLower or contentTypeLower in HLS_CONTENT_TYPES:
			return "hls", "hls"
		for delivery in deliveryNodes:
			typeName = attrValue(delivery, ["type", "extensionName"], "").lower()
			if "m3u8" in typeName or "mpegurl" in typeName:
				return "hls", "hls"
		if "identifierbaseddeliveryparameters" in names or identifier:
			return "identifier", "identifier"
		if "otherdeliveryparameters" in names:
			return "ip", "other"
		if sourceTypeLower.endswith(":dvb-dash"):
			return "dash", "dash"
		if sourceTypeLower.endswith(":dvb-t"):
			return "dvb-t", "dvb-t"
		if sourceTypeLower.endswith(":dvb-s"):
			return "dvb-s", "dvb-s"
		if sourceTypeLower.endswith(":dvb-c"):
			return "dvb-c", "dvb-c"
		if linkedApplications or sourceTypeLower.endswith(":application"):
			return "application", "application"
		return "unknown", "unknown"

	def flags(self, service):
		flags = []
		if any(instance.drm for instance in service.instances):
			flags.append("drm")
		if service.linkedApplications or any(instance.hbbtv for instance in service.instances):
			flags.append("hbbtv")
		if any(instance.instanceType in ("dash", "hls", "ip", "radio", "identifier", "rtsp", "multicast") for instance in service.instances):
			flags.append("ip")
		if any(instance.instanceType in ("dvb-s", "dvb-c", "dvb-t", "broadcast") for instance in service.instances):
			flags.append("broadcast")
		if service.contentGuideSourceRef or service.contentGuideServiceRef:
			flags.append("content_guide")
		return flags


SERVICE_LIST_LOGO_RELATION_SUFFIX = ":1001.1"


def httpUrl(value):
	value = (value or "").strip()
	parsed = urlsplit(value)
	if parsed.scheme.lower() in ("http", "https") and parsed.hostname and parsed.username is None:
		return value
	return ""


def unique(values):
	result = []
	for value in values:
		if value and value not in result:
			result.append(value)
	return result


def valueList(value):
	if value is None or value == "":
		return []
	if isinstance(value, (list, tuple)):
		return list(value)
	return [value]


def texts(element, childName):
	return [textValue(child, "") for child in iterDescendants(element, [childName]) if textValue(child, "")]


class CsrClient:
	"""Query a registry and preserve the identity and provenance of offerings."""

	def __init__(self, cacheDir):
		self.fetcher = ServiceListFetcher(cacheDir)

	def query(self, endpoint, targetCountry="", language="", delivery="", providerName="", regulatorLists=False, force=False):
		# TS 103 770 defines a canonical parameter order for SLR queries.
		params = []
		if targetCountry:
			params.append(("TargetCountry", targetCountry))
		if regulatorLists:
			params.append(("regulatorListFlag", "true"))
		if delivery:
			params.append(("Delivery", delivery))
		if language:
			params.append(("Language", language))
		if providerName:
			params.append(("ProviderName", providerName))

		parsedEndpoint = urlsplit(endpoint)
		query = parseQsl(parsedEndpoint.query, keep_blank_values=True)
		query.extend(params)
		queryUrl = urlunsplit(
			(
				parsedEndpoint.scheme,
				parsedEndpoint.netloc,
				parsedEndpoint.path,
				urlencode(query),
				parsedEndpoint.fragment,
			)
		)
		result = self.fetcher.fetch(queryUrl, force=force)
		offerings = self.parseOfferings(result.content, language=language)
		for offering in offerings:
			offering["registry_url"] = endpoint
			offering["registry_query_url"] = queryUrl
			offering["registry_http_status"] = result.status
			offering["registry_cache_stale"] = result.stale
			if regulatorLists:
				offering["regulator_list"] = True
		return offerings

	def parseOfferings(self, content, language=""):
		def parseJson(content):
			if isinstance(content, bytes):
				content = content.decode("utf-8", "replace")
			data = jsonLoads(content)
			candidates = data if isinstance(data, list) else data.get("offerings", data.get("serviceLists", []))
			offerings = []
			for candidate in candidates if isinstance(candidates, list) else []:
				if not isinstance(candidate, dict):
					continue
				url = httpUrl(candidate.get("serviceListURI") or candidate.get("serviceListUri") or candidate.get("url"))
				if not url:
					continue
				offerings.append(
					{
						"id": str(candidate.get("serviceListId") or candidate.get("id") or ""),
						"name": str(candidate.get("serviceListName") or candidate.get("name") or ""),
						"names": [],
						"url": url,
						"urls": [url],
						"provider": str(candidate.get("providerName") or candidate.get("provider") or ""),
						"provider_names": [],
						"jurisdiction": str(candidate.get("jurisdiction") or ""),
						"target_countries": valueList(candidate.get("targetCountries")),
						"languages": valueList(candidate.get("languages")),
						"delivery": valueList(candidate.get("delivery")),
						"logo_urls": [],
						"regulator_list": bool(candidate.get("regulatorList", False)),
					}
				)
			return offerings

		def parseXml(content, language):
			def parseServiceListOffering(element, provider, providerNames, jurisdiction, language):
				names = self.namedValues(element, "ServiceListName")
				urls = []
				for uriContainer in iterDescendants(element, ["ServiceListURI"]):
					for uri in iterDescendants(uriContainer, ["URI"]):
						candidate = httpUrl(textValue(uri, ""))
						if candidate:
							urls.append(candidate)

				listIds = texts(element, "ServiceListId")
				logoUrls = []
				for related in iterDescendants(element, ["RelatedMaterial"]):
					relations = [attrValue(item, ["href"], "") for item in iterDescendants(related, ["HowRelated"])]
					if not any(value.endswith(SERVICE_LIST_LOGO_RELATION_SUFFIX) for value in relations):
						continue
					for mediaUri in iterDescendants(related, ["MediaUri"]):
						contentType = attrValue(mediaUri, ["contentType"], "").lower()
						candidate = httpUrl(textValue(mediaUri, ""))
						if candidate and (not contentType or contentType.startswith("image/")):
							logoUrls.append(candidate)

				delivery = []
				for deliveryElement in iterDescendants(element, ["Delivery"]):
					for child in iterChildren(deliveryElement):
						name = localName(child.tag)
						if name.endswith("Delivery") and name not in delivery:
							delivery.append(name)

				return {
					"id": listIds[0] if listIds else "",
					"name": self.preferredNamedValue(names, language),
					"names": names,
					"url": urls[0] if urls else "",
					"urls": unique(urls),
					"provider": provider,
					"provider_names": providerNames,
					"jurisdiction": jurisdiction,
					"target_countries": unique(texts(element, "TargetCountry")),
					"languages": unique(texts(element, "Language")),
					"delivery": delivery,
					"logo_urls": unique(logoUrls),
					"regulator_list": str(attrValue(element, ["regulatorListFlag"], "")).lower() in ("1", "true", "yes"),
				}

			root = fromstring(content)
			offerings = []
			for wrapper in root.iter():
				wrapperName = localName(wrapper.tag)
				if wrapperName not in ("ProviderOffering", "RegulatorOffering"):
					continue

				providerElement = None
				for child in iterChildren(wrapper):
					if localName(child.tag) in ("Provider", "Regulator"):
						providerElement = child
						break
				providerNames = self.namedValues(providerElement, "Name") if providerElement is not None else []
				provider = self.preferredNamedValue(providerNames, language)
				jurisdiction = ""
				if providerElement is not None:
					values = texts(providerElement, "AdministrativeUnit")
					jurisdiction = values[0] if values else ""

				for serviceList in iterChildren(wrapper, ["ServiceListOffering"]):
					item = parseServiceListOffering(serviceList, provider, providerNames, jurisdiction, language)
					item["regulator_list"] = bool(item.get("regulator_list")) or wrapperName == "RegulatorOffering"
					if item.get("url"):
						offerings.append(item)
			return offerings

		if isinstance(content, bytes):
			stripped = content.lstrip()
		else:
			stripped = str(content).lstrip().encode("utf-8")
		if not stripped:
			return []
		if stripped.startswith((b"{", b"[")):
			return parseJson(content)
		return parseXml(content, language)

	def namedValues(self, element, name):
		if element is None:
			return []
		values = []
		for child in iterDescendants(element, [name]):
			value = textValue(child, "")
			if not value:
				continue
			language = attrValue(child, ["lang", "xml:lang"], "")
			nameType = attrValue(child, ["type"], "")
			values.append({"value": value, "language": language, "type": nameType})
		return values

	def preferredNamedValue(self, values, language):
		if not values:
			return ""
		normalized = (language or "").lower().split("-", 1)[0]
		if normalized:
			for item in values:
				if item.get("language", "").lower().split("-", 1)[0] == normalized:
					return item["value"]
		for item in values:
			if item.get("type") == "main":
				return item["value"]
		return values[0]["value"]


# Stream types and FTA manifest inspection


DASH_CONTENT_TYPES = {"application/dash+xml"}
HLS_CONTENT_TYPES = {
	"application/vnd.apple.mpegurl",
	"application/x-mpegurl",
	"audio/mpegurl",
	"audio/x-mpegurl",
}
TS_CONTENT_TYPES = {"video/mp2t"}


def playbackUrl(instance):
	"""Use only an import-confirmed redirect target; retain the signalled URL."""
	original = str(getattr(instance, "url", "") or "")
	probe = (getattr(instance, "raw", None) or {}).get("media_probe", {})
	if probe.get("status") == "confirmed":
		target = str(probe.get("final_url") or "")
		parsed = urlsplit(target)
		if (
			parsed.scheme in ("http", "https")
			and parsed.hostname
			and parsed.username is None
			and parsed.password is None
			and (urlsplit(original).scheme != "https" or parsed.scheme == "https")
		):
			return target
	return original


def effectiveMediaKind(instance):
	"""Return detected media kind, falling back only to safe declarations."""
	detected = str(getattr(instance, "detectedMediaKind", "") or "").strip().lower()
	if detected:
		return detected
	declared = str(getattr(instance, "instanceType", "") or "").strip().lower()
	if declared in ("dash", "hls"):
		return declared
	if declared == "radio":
		return "radio"
	if declared == "rtsp":
		return "rtsp"
	if declared == "multicast":
		return "multicast-ts"
	return "unknown"


class MediaProbe:
	"""Inspect a small prefix of HTTP streams outside the Enigma2 process."""

	DEFAULT_MAX_BYTES = 64 * 1024
	DEFAULT_TIMEOUT = 6.0
	DEFAULT_MAX_WORKERS = 4
	DEFAULT_MAX_URLS = 1024
	HARD_MAX_BYTES = 256 * 1024
	HARD_MAX_WORKERS = 8
	HARD_MAX_URLS = 2048

	def __init__(self, opener=None, maxBytes=DEFAULT_MAX_BYTES, timeout=DEFAULT_TIMEOUT, maxWorkers=DEFAULT_MAX_WORKERS, maxUrls=DEFAULT_MAX_URLS):
		self.opener = opener or urlopen
		self.maxBytes = max(4096, min(int(maxBytes), self.HARD_MAX_BYTES))
		self.timeout = max(0.2, min(float(timeout), 30.0))
		self.maxWorkers = max(1, min(int(maxWorkers), self.HARD_MAX_WORKERS))
		self.maxUrls = max(1, min(int(maxUrls), self.HARD_MAX_URLS))

	@staticmethod
	def getInstances(value):
		if value is None:
			return
		if hasattr(value, "instanceType"):
			yield value
			return
		if hasattr(value, "services"):
			value = value.services
		elif hasattr(value, "instances"):
			value = value.instances
		try:
			iterator = iter(value)
		except TypeError:
			return
		for item in iterator:
			if hasattr(item, "instanceType"):
				yield item
			elif hasattr(item, "instances"):
				yield from item.instances

	@staticmethod
	def urlHint(url):
		path = urlsplit(url).path.lower()
		if path.endswith(".mpd"):
			return "dash"
		if path.endswith(".m3u8"):
			return "hls"
		return ""

	@staticmethod
	def declaredHint(instance):
		declared = str(getattr(instance, "instanceType", "") or "").lower()
		contentType = str(getattr(instance, "contentType", "") or "").split(";", 1)[0].lower()
		if declared in ("dash", "hls"):
			return declared
		if contentType in DASH_CONTENT_TYPES:
			return "dash"
		if contentType in HLS_CONTENT_TYPES:
			return "hls"
		return MediaProbe.urlHint(str(getattr(instance, "url", "") or ""))

	@staticmethod
	def hasTsSync(content):
		# Require four equally spaced sync bytes.  Scanning one full packet
		# covers 188-byte TS, 192-byte M2TS and 204-byte FEC packet layouts.
		for packetSize in (188, 192, 204):
			needed = packetSize * 3 + 1
			if len(content) < needed:
				continue
			for offset in range(min(packetSize, len(content))):
				if all(offset + packetSize * index < len(content) and content[offset + packetSize * index] == 0x47 for index in range(4)):
					return True, packetSize, offset
		return False, 0, 0

	@staticmethod
	def detect(content, contentType="", url=""):
		"""Classify a bounded byte prefix; URL is a hint, never TS proof."""
		content = bytes(content or b"")
		contentType = str(contentType or "").split(";", 1)[0].strip().lower()
		stripped = content.lstrip(b"\xef\xbb\xbf \t\r\n")
		if stripped[:7].upper() == b"#EXTM3U":
			return {"kind": "hls", "status": "confirmed", "evidence": "body_hls"}

		xmlHead = stripped[:8192]
		xmlHead = reSub(rb"^<\?xml[^>]*>\s*", b"", xmlHead, flags=IGNORECASE)
		if reMatch(rb"^<(?:[A-Za-z0-9_.-]+:)?MPD(?:\s|>)", xmlHead, flags=IGNORECASE):
			return {"kind": "dash", "status": "confirmed", "evidence": "body_mpd"}

		isTs, packetSize, offset = MediaProbe.hasTsSync(content)
		if isTs:
			return {
				"kind": "mpeg-ts",
				"status": "confirmed",
				"evidence": "ts_sync_{0}_offset_{1}".format(packetSize, offset),
			}
		if len(content) >= 12 and content[4:8] == b"ftyp":
			return {"kind": "progressive", "status": "confirmed", "evidence": "iso_bmff_ftyp"}
		if b"\x00\x00\x01\xba" in content[:64]:
			return {"kind": "progressive", "status": "confirmed", "evidence": "mpeg_ps_pack"}

		if contentType in DASH_CONTENT_TYPES:
			return {"kind": "dash", "status": "hint", "evidence": "http_content_type"}
		if contentType in HLS_CONTENT_TYPES:
			return {"kind": "hls", "status": "hint", "evidence": "http_content_type"}
		if contentType in TS_CONTENT_TYPES:
			return {
				"kind": "unknown",
				"status": "unconfirmed",
				"evidence": "mpeg_ts_mime_without_sync",
			}
		if contentType.startswith(("video/", "audio/")):
			return {"kind": "progressive", "status": "hint", "evidence": "http_content_type"}
		hint = MediaProbe.urlHint(url)
		if hint:
			return {"kind": hint, "status": "hint", "evidence": "url_suffix"}
		return {"kind": "unknown", "status": "unknown", "evidence": "no_signature"}

	@staticmethod
	def safeError(error):
		return str(error).replace("\r", " ").replace("\n", " ")[:200]

	def failure(self, instance, error):
		if isinstance(error, HTTPError):
			status = "http_{0}".format(error.code)
		elif isinstance(error, (socketTimeout, TimeoutError)):
			status = "timeout"
		elif isinstance(error, URLError):
			reason = getattr(error, "reason", None)
			status = "timeout" if isinstance(reason, (socketTimeout, TimeoutError)) else "network_error"
		else:
			status = "probe_error"
		return {
			"kind": self.declaredHint(instance) or "unknown",
			"status": status,
			"evidence": "declaration_or_url_hint" if self.declaredHint(instance) else "none",
			"content_type": "",
			"error": self.safeError(error),
		}

	@staticmethod
	def apply(instance, result):
		instance.detectedMediaKind = str(result.get("kind") or "unknown")
		instance.detectedContentType = str(result.get("content_type") or "")
		instance.mediaProbeStatus = str(result.get("status") or "probe_error")
		instance.mediaProbeEvidence = str(result.get("evidence") or "")
		raw = getattr(instance, "raw", None)
		if not isinstance(raw, dict):
			raw = {}
			instance.raw = raw
		raw["media_probe"] = dict(result)

	def inspect(self, value):
		def probeUrl(url):
			def responseContentType(headers):
				try:
					items = headers.items()
				except Exception:
					items = []
				for key, value in items:
					if str(key).lower() == "content-type":
						return str(value or "").split(";", 1)[0].strip().lower()
				return ""

			parsed = urlsplit(url)
			if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
				raise ValueError(_("media probe accepts only HTTP(S) URLs"))
			if parsed.username is not None or parsed.password is not None:
				raise ValueError(_("credentials in media URLs are not supported"))

			request = Request(url)
			request.add_header("User-Agent", "OpenATV-DvbIManager/0.3.6")
			# Detection uses the body, so no format negotiation is needed.
			request.add_header("Accept", "*/*")
			# Fetch manifests normally; the bounded read below still caps buffering.
			if not self.urlHint(url):
				request.add_header("Range", "bytes=0-{0}".format(self.maxBytes - 1))
			requestHeaders = dict(request.header_items())
			response = self.opener(request, timeout=self.timeout)
			try:
				finalUrl = str(response.geturl() or url)
				final = urlsplit(finalUrl)
				if parsed.scheme.lower() == "https" and final.scheme.lower() == "http":
					# Some official redirectors still advertise HTTP. Verify the same
					# endpoint over TLS once; never pass a downgraded URL to the player.
					response.close()
					secureUrl = urlunsplit(final._replace(scheme="https"))
					# urllib adds Host to the original request during open; never copy
					# that origin-specific header to a different CDN host.
					response = self.opener(Request(secureUrl, headers=requestHeaders), timeout=self.timeout)
					finalUrl = str(response.geturl() or secureUrl)
					final = urlsplit(finalUrl)
				if final.scheme.lower() not in ("http", "https") or not final.hostname:
					raise ValueError(_("media probe redirect left HTTP(S)"))
				if final.username is not None or final.password is not None:
					raise ValueError(_("credentials in redirected media URLs are not supported"))
				if parsed.scheme.lower() == "https" and final.scheme.lower() != "https":
					raise ValueError(_("media probe rejected HTTPS downgrade redirect"))
				contentType = responseContentType(getattr(response, "headers", {}))
				content = response.read(self.maxBytes + 1)
				truncated = len(content) > self.maxBytes
				content = content[: self.maxBytes]
				result = self.detect(content, contentType=contentType, url=finalUrl)
				result.update(
					{
						"content_type": contentType,
						"http_status": int(getattr(response, "status", 200) or 0),
						"bytes_read": len(content),
						"truncated": truncated,
						"final_url": finalUrl,
					}
				)
				return result
			finally:
				response.close()

		instances = list(self.getInstances(value))
		groups = {}
		report = {"eligible_instances": 0, "scheduled_urls": 0, "counts": {}, "results": []}
		for instance in instances:
			url = str(getattr(instance, "url", "") or "").strip()
			scheme = urlsplit(url).scheme.lower() if url else ""
			if scheme not in ("http", "https"):
				kind = effectiveMediaKind(instance)
				result = {
					"kind": kind,
					"status": "not_http",
					"evidence": "declared_delivery",
					"content_type": "",
				}
				self.apply(instance, result)
				continue
			report["eligible_instances"] += 1
			if url not in groups and len(groups) >= self.maxUrls:
				result = {
					"kind": self.declaredHint(instance) or "unknown",
					"status": "skipped_limit",
					"evidence": "declaration_or_url_hint",
					"content_type": "",
				}
				self.apply(instance, result)
				continue
			groups.setdefault(url, []).append(instance)

		report["scheduled_urls"] = len(groups)
		if groups:
			workers = min(self.maxWorkers, len(groups))
			with ThreadPoolExecutor(max_workers=workers) as executor:
				pending = {executor.submit(probeUrl, url): (url, grouped) for url, grouped in groups.items()}
				for future in asCompleted(pending):
					url, grouped = pending[future]
					try:
						result = future.result()
					except Exception as error:
						for instance in grouped:
							failure = self.failure(instance, error)
							self.apply(instance, failure)
							self.record(report, url, failure)
						continue
					for instance in grouped:
						self.apply(instance, result)
						self.record(report, url, result)
		return report

	@staticmethod
	def record(report, url, result):
		status = str(result.get("status") or "probe_error")
		report["counts"][status] = report["counts"].get(status, 0) + 1
		report["results"].append(
			{
				"url": url,
				"kind": result.get("kind", "unknown"),
				"status": status,
				"evidence": result.get("evidence", ""),
				"content_type": result.get("content_type", ""),
				"http_status": result.get("http_status"),
				"error": result.get("error", ""),
			}
		)


class ManifestParseError(ValueError):
	"""The downloaded resource is not a safely understood DASH/HLS manifest."""


class ManifestInspector:
	"""Inspect a bounded set of IP delivery manifests for content protection.

	Network work runs in the DVB-I background task, never in the
	Enigma2 UI process.  Downloads go through ServiceListFetcher so manifests
	receive the same HTTP validation, conditional requests and last-good cache
	behaviour as service lists.
	"""

	DEFAULT_MAX_MANIFESTS = 64
	DEFAULT_MAX_WORKERS = 4
	DEFAULT_TIMEOUT = 8.0
	DEFAULT_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
	DEFAULT_MAX_HLS_VARIANTS = 16

	# Hard ceilings protect a box even if a corrupt job/configuration supplies
	# unreasonable values.  The lower caller supplied limits still apply.
	HARD_MAX_MANIFESTS = 1024
	HARD_MAX_WORKERS = 8
	HARD_MAX_TIMEOUT = 30.0
	HARD_MAX_MANIFEST_BYTES = 8 * 1024 * 1024
	HARD_MAX_HLS_VARIANTS = 16

	UUID_RE = reCompile(
		r"(?:urn:uuid:)?\{?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\}?",
		IGNORECASE,
	)

	KNOWN_KEY_FORMATS = {
		"com.widevine": "EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED",
		"com.widevine.alpha": "EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED",
		"com.microsoft.playready": "9A04F079-9840-4286-AB92-E65BE0885F95",
		"com.apple.streamingkeydelivery": "94CE86FB-07FF-4F43-ADB8-93D2FA968CA2",
		"org.w3.clearkey": "E2719D58-A985-B3C9-781A-B030AF78D30E",
	}

	def __init__(self, cacheDir=None, fetcher=None, maxManifests=DEFAULT_MAX_MANIFESTS, maxWorkers=DEFAULT_MAX_WORKERS, timeout=DEFAULT_TIMEOUT, maxManifestBytes=DEFAULT_MAX_MANIFEST_BYTES, maxHlsVariants=DEFAULT_MAX_HLS_VARIANTS, mediaOpener=None, clock=None):
		def boundedFloat(value, minimum, maximum):
			try:
				parsed = float(value)
			except (TypeError, ValueError):
				parsed = minimum
			return max(minimum, min(parsed, maximum))

		if fetcher is None:
			if not cacheDir:
				raise ValueError(_("cache_dir or fetcher is required"))
			fetcher = ServiceListFetcher(cacheDir)
		if not hasattr(fetcher, "fetch"):
			raise TypeError(_("fetcher must provide fetch(url, timeout, force)"))

		self.fetcher = fetcher
		self.mediaOpener = mediaOpener or urlopen
		self.clock = clock or time
		self.maxManifests = self.boundedInt(maxManifests, 1, self.HARD_MAX_MANIFESTS)
		self.maxWorkers = self.boundedInt(maxWorkers, 1, self.HARD_MAX_WORKERS)
		self.timeout = boundedFloat(timeout, 0.1, self.HARD_MAX_TIMEOUT)
		self.maxManifestBytes = self.boundedInt(maxManifestBytes, 1024, self.HARD_MAX_MANIFEST_BYTES)
		self.maxHlsVariants = self.boundedInt(maxHlsVariants, 1, self.HARD_MAX_HLS_VARIANTS)

	@staticmethod
	def boundedInt(value, minimum, maximum):
		try:
			parsed = int(value)
		except (TypeError, ValueError):
			parsed = minimum
		return max(minimum, min(parsed, maximum))

	@staticmethod
	def kind(instance):
		detected = getattr(instance, "detectedMediaKind", "") or ""
		if str(detected).strip().lower() in ("dash", "hls"):
			return str(detected).strip().lower()
		value = getattr(instance, "instanceType", "") or ""
		return str(value).strip().lower()

	def getInstances(self, value):
		"""Yield service instances from a list, service list, service or instance."""
		if value is None:
			return
		if hasattr(value, "instanceType"):
			yield value
			return
		if hasattr(value, "services"):
			value = getattr(value, "services", [])
		elif hasattr(value, "instances"):
			value = getattr(value, "instances", [])

		try:
			iterator = iter(value)
		except TypeError:
			return
		for item in iterator:
			if hasattr(item, "instanceType"):
				yield item
			elif hasattr(item, "instances"):
				yield from item.instances

	def inspect(self, value):
		"""Inspect a service list, services or instances and return a JSON-safe report."""
		return self.inspectServices(value)

	def inspectServiceList(self, serviceList):
		return self.inspectServices(serviceList)

	def inspectServices(self, services):
		candidates = [instance for instance in self.getInstances(services) if self.kind(instance) in ("dash", "hls")]
		report = {
			"eligible_instances": len(candidates),
			"scheduled_manifests": 0,
			"max_manifests": self.maxManifests,
			"max_workers": self.maxWorkers,
			"max_hls_variants": self.maxHlsVariants,
			"timeout": self.timeout,
			"counts": {},
			"results": [],
		}

		groups = {}
		rejectedUrls = set()
		for instance in candidates:
			url = playbackUrl(instance).strip()
			if not url:
				self.applyResult(instance, self.makeResult("missing_url"), report)
				continue

			if url in groups:
				groups[url].append(instance)
				continue
			if url in rejectedUrls or len(groups) >= self.maxManifests:
				rejectedUrls.add(url)
				self.applyResult(instance, self.makeResult("skipped_limit"), report)
				continue
			groups[url] = [instance]

		report["scheduled_manifests"] = len(groups)
		if not groups:
			return report

		workers = min(self.maxWorkers, len(groups))
		with ThreadPoolExecutor(max_workers=workers) as executor:
			pending = {}
			for url, instances in groups.items():
				kinds = sorted(set(self.kind(instance) for instance in instances))
				pending[executor.submit(self.inspectUrl, url, kinds)] = (url, instances)

			for future in asCompleted(pending):
				url, instances = pending[future]
				try:
					byKind = future.result()
				except Exception as error:
					# This last guard keeps an unexpected implementation error
					# local to the affected URL/instances.
					byKind = {self.kind(instance): self.makeResult("probe_error", error=self.safeError(error)) for instance in instances}
				for instance in instances:
					result = byKind.get(self.kind(instance))
					if result is None:
						result = self.makeResult("probe_error", error="missing parser result")
					self.applyResult(instance, result, report, url=url)

		return report

	def inspectUrl(self, url, kinds, force=False):
		def parseDash(content):
			# Scan the complete, already size-bounded document.  XML permits
			# leading whitespace before a DTD, so checking only a prefix would
			# leave an avoidable parser-hardening gap.
			def dashSystemIds(element):
				def systemIdFromPssh(encoded):
					try:
						compact = "".join(str(encoded).split())
						compact += "=" * ((4 - len(compact) % 4) % 4)
						payload = b64decode(compact.encode("ascii"), validate=True)
					except (ValueError, TypeError, UnicodeError):
						return ""

					marker = payload.find(b"pssh")
					if marker < 4 or len(payload) < marker + 24:
						return ""
					try:
						return str(UUID(bytes=payload[marker + 8: marker + 24])).upper()
					except (ValueError, AttributeError):
						return ""

				cls = type(self)
				identifiers = []
				scheme = cls.attribute(element, "schemeIdUri")
				value = cls.attribute(element, "value")
				schemeLower = scheme.casefold()

				if schemeLower == "urn:mpeg:dash:mp4protection:2011":
					cls.appendUnique(identifiers, (value or "CENC").upper())
				elif scheme:
					cls.appendUnique(identifiers, cls.normaliseSystemId(scheme))

				for descendant in element.iter():
					local = cls.localName(descendant.tag)
					if local == "pssh" and descendant.text:
						cls.appendUnique(identifiers, systemIdFromPssh(descendant.text))
					elif local in ("pro", "prheader"):
						cls.appendUnique(identifiers, cls.KNOWN_KEY_FORMATS["com.microsoft.playready"])

				return identifiers, scheme, value

			lowered = content.lower()
			if b"<!doctype" in lowered or b"<!entity" in lowered:
				raise ManifestParseError(_("DTD/entity declarations are not accepted"))

			root = fromstring(content)
			if self.localName(root.tag) != "mpd":
				raise ManifestParseError(_("resource is not a DASH MPD"))

			systemIds = []
			protection = []
			for element in root.iter():
				if self.localName(element.tag) != "contentprotection":
					continue
				identifiers, scheme, value = dashSystemIds(element)
				for systemId in identifiers:
					self.appendUnique(systemIds, systemId)
				protection.append(
					{
						"kind": "drm",
						"source": "dash_manifest",
						"scheme_id_uri": scheme,
						"value": value,
						"system_ids": identifiers,
					}
				)

			drm = bool(protection)
			return self.makeResult(
				"drm_required" if drm else "clear",
				drm=drm,
				systemIds=systemIds,
				protection=protection,
				vod=root.get("type", "").strip() == "static",
			)

		try:
			fetched = self.fetcher.fetch(url, timeout=self.timeout, force=force)
			status = int(getattr(fetched, "status", 200) or 0)
			if status >= 400:
				return {kind: self.makeResult("http_{0}".format(status)) for kind in kinds}
			content = getattr(fetched, "content", b"")
			if isinstance(content, str):
				content = content.encode("utf-8")
			if not isinstance(content, (bytes, bytearray)):
				raise ValueError(_("fetcher returned non-byte manifest content"))
			content = bytes(content)
			if len(content) > self.maxManifestBytes:
				raise ValueError(_("manifest exceeds the inspection size limit"))

			metadata = {
				"http_status": status,
				"stale": bool(getattr(fetched, "stale", False)),
				"changed": bool(getattr(fetched, "changed", False)),
			}
			results = {}
			for kind in kinds:
				try:
					if kind == "dash":
						parsed = parseDash(content)
						# Never reject a live origin using segment names from an
						# old local cache. Revalidate once before checking media.
						if parsed["status"] == "clear" and not force and (status == 304 or metadata["stale"]) and fromstring(content).get("type") == "dynamic":
							return self.inspectUrl(url, kinds, force=True)
						if parsed["status"] == "clear" and status == 200 and not metadata["stale"]:
							parsed["availability"] = self.dashAvailability(url, content)
					elif kind == "hls":
						parsed = self.inspectHls(url, content)
					else:
						parsed = self.makeResult("not_applicable")
					parsed.update(metadata)
					results[kind] = parsed
				except (ParseError, ManifestParseError, UnicodeError, ValueError) as error:
					result = self.makeResult("parse_error", error=self.safeError(error))
					result.update(metadata)
					results[kind] = result
			return results
		except HTTPError as error:
			return {kind: self.makeResult("http_{0}".format(error.code), error=self.safeError(error)) for kind in kinds}
		except (socketTimeout, TimeoutError) as error:
			return {kind: self.makeResult("timeout", error=self.safeError(error)) for kind in kinds}
		except URLError as error:
			reason = getattr(error, "reason", None)
			status = "timeout" if isinstance(reason, (socketTimeout, TimeoutError)) else "network_error"
			return {kind: self.makeResult(status, error=self.safeError(error)) for kind in kinds}
		except Exception as error:
			return {kind: self.makeResult("fetch_error", error=self.safeError(error)) for kind in kinds}

	@staticmethod
	def safeError(error):
		return str(error).replace("\r", " ").replace("\n", " ")[:200]

	def dashAvailability(self, url, content):
		"""Check old live Number/duration MPDs, never infer failure from age alone.

		Only missing media on every advertised base excludes this instance.
		Unsupported layouts, clock uncertainty and network errors stay unknown.
		Runs in the importer worker, not on zap; at most eight 64-byte requests.
		"""
		unknown = {"status": "unknown"}
		checks = []

		def children(node, name):
			return [item for item in node if self.localName(item.tag) == name]

		def seconds(value):
			match = reFullmatch(r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", value)
			if not match or not any(match.groups()):
				raise ValueError(_("unsupported duration"))
			return sum(float(item or 0) * unit for item, unit in zip(match.groups(), (3600, 60, 1)))

		def timestamp(value):
			parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
			if parsed.tzinfo is None:
				raise ValueError(_("timezone required"))
			return parsed.timestamp()

		try:
			root = fromstring(content)
			if root.get("type") != "dynamic":
				return unknown
			now = self.clock()
			if now - timestamp(root.attrib["publishTime"]) < max(120, 3 * seconds(root.get("minimumUpdatePeriod", "PT30S"))):
				return unknown
			periods = children(root, "period")
			if len(periods) != 1:
				return unknown
			period = periods[0]
			adaptations = [
				item for item in children(period, "adaptationset") if item.get("contentType") == "video" or item.get("mimeType", "").startswith("video/")
			]
			if not adaptations:
				return unknown
			adaptation = adaptations[0]
			representations = children(adaptation, "representation")
			if not representations:
				return unknown
			representation = representations[0]
			template = {}
			bases = [url]
			for node in (root, period, adaptation, representation):
				advertised = children(node, "baseurl")
				if advertised:
					bases = list(dict.fromkeys(self.resolveHlsReference(base, item.text or "") for base in bases for item in advertised))
					if len(bases) > 4:
						return unknown
				for item in children(node, "segmenttemplate"):
					if children(item, "segmenttimeline"):
						return unknown
					template.update(item.attrib)
			if int(template.get("presentationTimeOffset", "0")) or "availabilityTimeOffset" in template:
				return unknown
			duration = int(template["duration"]) / int(template.get("timescale", "1"))
			elapsed = now - timestamp(root.attrib["availabilityStartTime"]) - seconds(period.get("start", "PT0S"))
			window = seconds(root.get("timeShiftBufferDepth", "PT5M"))
			if not isfinite(duration) or not 0.1 <= duration <= 60 or elapsed < 120 or window < duration * 8:
				return unknown
			delays = (min(max(12, 3 * duration), window / 4), min(max(60, 30 * duration), window / 2))
			media = template["media"].replace("$Bandwidth$", representation.get("bandwidth", "")).replace("$RepresentationID$", representation.get("id", ""))
			for base in bases:
				for delay in delays:
					number = int(template.get("startNumber", "1")) + int((elapsed - delay) // duration)
					path = reSub(r"\$Number(?:%0(\d{1,2})d)?\$", lambda match, number=number: str(number).zfill(min(20, int(match.group(1) or 0))), media)
					if "$" in path or path == media:
						return unknown
					target = self.resolveHlsReference(base, path)
					try:
						response = self.mediaOpener(
							Request(target, headers={"Range": "bytes=0-63", "User-Agent": "OpenATV-DvbIManager/0.3.6"}), timeout=min(self.timeout, 3)
						)
						try:
							self.resolveHlsReference(target, response.geturl())
							status = int(response.status)
							body = response.read(64)
						finally:
							response.close()
						if status in (200, 206) and body:
							return {"status": "available", "checks": checks + [{"url": target, "status": status}]}
					except HTTPError as error:
						status = error.code
						error.close()
					checks.append({"url": target, "status": status})
			return {"status": "unavailable" if checks and all(item["status"] in (404, 410) for item in checks) else "unknown", "checks": checks}
		except (KeyError, ValueError, TypeError, ArithmeticError, OSError, URLError):
			return {"status": "unknown", "checks": checks}

	@staticmethod
	def makeResult(status, drm=False, systemIds=None, protection=None, error="", vod=False):
		return {
			"status": status,
			"vod": bool(vod),
			"drm": bool(drm),
			"system_ids": list(systemIds or []),
			"content_protection": list(protection or []),
			"error": error,
		}

	@staticmethod
	def localName(name):
		value = str(name or "")
		if "}" in value:
			value = value.rsplit("}", 1)[1]
		if ":" in value:
			value = value.rsplit(":", 1)[1]
		return value.lower()

	@classmethod
	def attribute(cls, element, name, default=""):
		expected = name.lower()
		for key, value in element.attrib.items():
			if cls.localName(key) == expected:
				return str(value or "").strip()
		return default

	@staticmethod
	def appendUnique(values, value):
		value = str(value or "").strip()
		if not value:
			return
		lowered = value.casefold()
		if all(str(current).casefold() != lowered for current in values):
			values.append(value)

	@classmethod
	def normaliseSystemId(cls, value):
		value = str(value or "").strip()
		if not value:
			return ""
		known = cls.KNOWN_KEY_FORMATS.get(value.casefold())
		if known:
			return known
		match = cls.UUID_RE.fullmatch(value)
		if match:
			return match.group(1).upper()
		return value

	@staticmethod
	def splitHlsAttributes(value):
		fields = []
		current = []
		quoteChar = None
		escaped = False
		for character in value:
			if escaped:
				current.append(character)
				escaped = False
			elif character == "\\" and quoteChar:
				current.append(character)
				escaped = True
			elif quoteChar:
				current.append(character)
				if character == quoteChar:
					quoteChar = None
			elif character in ('"', "'"):
				current.append(character)
				quoteChar = character
			elif character == ",":
				fields.append("".join(current).strip())
				current = []
			else:
				current.append(character)
		if quoteChar:
			raise ManifestParseError(_("unterminated HLS attribute quote"))
		fields.append("".join(current).strip())

		attributes = {}
		for field in fields:
			if not field:
				continue
			if "=" not in field:
				raise ManifestParseError(_("invalid HLS key attribute"))
			key, item = field.split("=", 1)
			key = key.strip().upper()
			item = item.strip()
			if not key:
				raise ManifestParseError(_("empty HLS key attribute"))
			if len(item) >= 2 and item[0] == item[-1] and item[0] in ('"', "'"):
				item = item[1:-1].replace("\\" + item[0], item[0])
			attributes[key] = item
		return attributes

	def parseHlsPlaylist(self, content):
		def hlsSystemId(method, keyFormat):
			cls = type(self)
			keyFormat = str(keyFormat or "identity").strip()
			if keyFormat.casefold() == "identity":
				return method.upper()
			return cls.normaliseSystemId(keyFormat)

		text = content.decode("utf-8-sig")
		lines = [line.strip() for line in text.splitlines() if line.strip()]
		if not lines or lines[0].upper() != "#EXTM3U":
			raise ManifestParseError(_("resource is not an HLS playlist"))

		systemIds = []
		protection = []
		references = []
		malformedKey = False
		expectsStreamUri = False
		playlistTypes = []
		for line in lines[1:]:
			upper = line.upper()
			if expectsStreamUri:
				if line.startswith("#"):
					raise ManifestParseError(_("HLS variant URI is missing"))
				references.append(line)
				expectsStreamUri = False
				continue

			if not (upper.startswith("#EXT-X-KEY:") or upper.startswith("#EXT-X-SESSION-KEY:")):
				if upper.startswith("#EXT-X-PLAYLIST-TYPE:"):
					playlistTypes.append(line.split(":", 1)[1].strip())
				elif upper.startswith("#EXT-X-STREAM-INF:"):
					expectsStreamUri = True
				elif upper.startswith(
					(
						"#EXT-X-MEDIA:",
						"#EXT-X-I-FRAME-STREAM-INF:",
						"#EXT-X-IMAGE-STREAM-INF:",
					)
				):
					unusedTag, attributesText = line.split(":", 1)
					attributes = self.splitHlsAttributes(attributesText)
					uri = attributes.get("URI", "").strip()
					if uri:
						references.append(uri)
					elif not upper.startswith("#EXT-X-MEDIA:"):
						raise ManifestParseError(_("HLS variant URI is missing"))
				continue
			unusedTag, attributesText = line.split(":", 1)
			try:
				attributes = self.splitHlsAttributes(attributesText)
			except ManifestParseError:
				malformedKey = True
				continue
			method = attributes.get("METHOD", "").strip().upper()
			if not method:
				malformedKey = True
				continue
			if method == "NONE":
				continue

			keyFormat = attributes.get("KEYFORMAT", "identity")
			systemId = hlsSystemId(method, keyFormat)
			self.appendUnique(systemIds, systemId)
			protection.append(
				{
					"kind": "drm",
					"source": "hls_manifest",
					"method": method,
					"key_format": keyFormat,
					"system_ids": [systemId] if systemId else [],
				}
			)

		if expectsStreamUri:
			raise ManifestParseError(_("HLS variant URI is missing"))
		if malformedKey and not protection:
			raise ManifestParseError(_("malformed HLS key declaration"))
		drm = bool(protection)
		parsed = self.makeResult(
			"drm_required" if drm else "clear",
			drm=drm,
			systemIds=systemIds,
			protection=protection,
			# ENDLIST alone also occurs when a live event ends.  Only an
			# explicit VOD declaration labels a media playlist as on demand.
			vod=playlistTypes == ["VOD"] and not references,
		)
		return parsed, references

	def parseHls(self, content):
		parsed, references = self.parseHlsPlaylist(content)
		if not parsed.get("drm") and references:
			# Parsing alone cannot certify a master playlist.  The network-aware
			# inspector below follows every unique referenced playlist while it
			# remains inside the configured hard bound.
			return self.makeResult("unverified_variant_keys")
		return parsed

	@staticmethod
	def resolveHlsReference(baseUrl, reference):
		reference = str(reference or "").strip()
		if not reference or len(reference) > 4096:
			raise ManifestParseError(_("invalid HLS variant URI"))
		if any(ord(character) < 32 or ord(character) == 127 for character in reference):
			raise ManifestParseError(_("invalid HLS variant URI"))

		base = urlsplit(baseUrl or "")
		resolved = urlsplit(urljoin(baseUrl, reference))
		if resolved.scheme.lower() not in ("http", "https") or not resolved.hostname:
			raise ManifestParseError(_("HLS variant URI must use HTTP or HTTPS"))
		if resolved.username is not None or resolved.password is not None:
			raise ManifestParseError(_("credentials in HLS variant URIs are not supported"))
		if base.scheme.lower() == "https" and resolved.scheme.lower() != "https":
			raise ManifestParseError(_("HTTPS HLS master may not downgrade a variant to HTTP"))
		return urlunsplit(
			(
				resolved.scheme,
				resolved.netloc,
				resolved.path,
				resolved.query,
				"",
			)
		)

	def hlsVariantResult(self, status, checked, urls, error=""):
		result = self.makeResult(status, error=error)
		result["hls_variants_checked"] = int(checked)
		result["hls_variant_urls"] = list(urls)
		return result

	def inspectHls(self, masterUrl, content):
		def fetchHlsVariant(url):
			try:
				fetched = self.fetcher.fetch(url, timeout=self.timeout, force=False)
				status = int(getattr(fetched, "status", 200) or 0)
				if status >= 400:
					return None, "HTTP {0}".format(status)
				content = getattr(fetched, "content", b"")
				if isinstance(content, str):
					content = content.encode("utf-8")
				if not isinstance(content, (bytes, bytearray)):
					return None, "fetcher returned non-byte manifest content"
				content = bytes(content)
				if len(content) > self.maxManifestBytes:
					return None, "manifest exceeds the inspection size limit"
				return content, ""
			except HTTPError as error:
				return None, "HTTP {0}".format(error.code)
			except (socketTimeout, TimeoutError) as error:
				return None, self.safeError(error) or "timeout"
			except URLError as error:
				return None, self.safeError(error) or "network error"
			except Exception as error:
				return None, self.safeError(error) or "fetch error"

		parsed, references = self.parseHlsPlaylist(content)
		if parsed.get("drm") or not references:
			parsed["hls_variants_checked"] = 0
			parsed["hls_variant_urls"] = []
			return parsed

		rootUrl = self.resolveHlsReference(masterUrl, masterUrl)
		queue = []
		queued = set()
		masterUrls = {rootUrl}
		checkedUrls = []
		mediaVod = []

		def enqueue(baseUrl, values):
			for value in values:
				try:
					resolved = self.resolveHlsReference(baseUrl, value)
				except ManifestParseError as error:
					return self.hlsVariantResult("unverified_variant_uri", len(checkedUrls), checkedUrls, self.safeError(error))
				if resolved in masterUrls:
					return self.hlsVariantResult(
						"unverified_variant_cycle",
						len(checkedUrls),
						checkedUrls,
						"HLS master playlist contains a reference cycle",
					)
				if resolved in queued or resolved in checkedUrls:
					continue
				if len(queued) + len(checkedUrls) >= self.maxHlsVariants:
					return self.hlsVariantResult(
						"unverified_variant_limit",
						len(checkedUrls),
						checkedUrls,
						"HLS variant inspection limit reached",
					)
				queued.add(resolved)
				queue.append(resolved)
			return None

		failure = enqueue(rootUrl, references)
		if failure:
			return failure

		while queue:
			variantUrl = queue.pop(0)
			queued.discard(variantUrl)
			variantContent, error = fetchHlsVariant(variantUrl)
			if variantContent is None:
				return self.hlsVariantResult("unverified_variant_fetch", len(checkedUrls), checkedUrls, error)
			checkedUrls.append(variantUrl)
			try:
				variant, nestedReferences = self.parseHlsPlaylist(variantContent)
			except (ManifestParseError, UnicodeError, ValueError) as error:
				return self.hlsVariantResult("unverified_variant_parse", len(checkedUrls), checkedUrls, self.safeError(error))

			if variant.get("drm"):
				variant["hls_variants_checked"] = len(checkedUrls)
				variant["hls_variant_urls"] = list(checkedUrls)
				variant["hls_protected_variant_url"] = variantUrl
				return variant
			if nestedReferences:
				masterUrls.add(variantUrl)
				failure = enqueue(variantUrl, nestedReferences)
				if failure:
					return failure
			else:
				mediaVod.append(bool(variant.get("vod")))

		# Reuse only the playlists already checked for encryption.  Every
		# referenced media playlist must explicitly declare VOD.
		result = self.makeResult("clear", vod=bool(mediaVod) and all(mediaVod))
		result["hls_variants_checked"] = len(checkedUrls)
		result["hls_variant_urls"] = list(checkedUrls)
		return result

	def applyResult(self, instance, result, report, url=""):
		status = str(result.get("status", "probe_error"))
		systemIds = list(result.get("system_ids", []))
		protection = list(result.get("content_protection", []))

		# Never clear DRM signalled by the Service List (or by a previous safe
		# positive inspection).  A negative/error probe only updates its own
		# status and cannot turn protected content into a false FTA result.
		if result.get("drm"):
			instance.drm = True
			currentIds = getattr(instance, "drmSystemIds", None)
			if currentIds is None:
				currentIds = []
				instance.drmSystemIds = currentIds
			for systemId in systemIds:
				self.appendUnique(currentIds, systemId)

			currentProtection = getattr(instance, "contentProtection", None)
			if currentProtection is None:
				currentProtection = []
				instance.contentProtection = currentProtection
			for item in protection:
				if item not in currentProtection:
					currentProtection.append(item)

		instance.manifestProbeStatus = status
		instance.manifestProbeDrm = bool(result.get("drm"))
		instance.manifestProbeSystemIds = systemIds
		instance.manifestMediaKind = self.kind(instance)
		instance.manifestVod = status == "clear" and bool(result.get("vod"))
		instance.playbackUnavailable = result.get("availability", {}).get("status") == "unavailable"

		raw = getattr(instance, "raw", None)
		if not isinstance(raw, dict):
			raw = {}
			instance.raw = raw
		probeData = {
			"status": status,
			"vod": instance.manifestVod,
			"drm": bool(result.get("drm")),
			"system_ids": systemIds,
			"http_status": result.get("http_status"),
			"stale": bool(result.get("stale", False)),
			"error": result.get("error", ""),
			"media_kind": self.kind(instance),
			"availability": result.get("availability", {"status": "unknown"}),
		}
		raw["manifest_probe"] = probeData

		report["counts"][status] = report["counts"].get(status, 0) + 1
		report["results"].append(
			{
				"url": url or str(getattr(instance, "url", "") or ""),
				"type": self.kind(instance),
				"declared_type": str(getattr(instance, "instanceType", "") or ""),
				"status": status,
				"vod": instance.manifestVod,
				"drm": bool(result.get("drm")),
				"system_ids": systemIds,
				"http_status": result.get("http_status"),
				"stale": bool(result.get("stale", False)),
			}
		)


def selectPlayer(instance, preferred="4097", available=(), automatic=True):
	kind = effectiveMediaKind(instance)
	scheme = urlsplit(instance.url or "").scheme.lower()
	confirmedTs = kind == "mpeg-ts" and getattr(instance, "mediaProbeStatus", "") == "confirmed" and scheme in ("http", "https")
	installed = set(str(item) for item in available)
	preferred = str(preferred or "4097")
	if not automatic and preferred != "auto":
		candidates = [preferred]
	elif preferred == "auto":
		candidates = (["1"] if confirmedTs else []) + ["4097", "5002", "5001"]
	else:
		candidates = [preferred] + (["1"] if confirmedTs else []) + ["4097", "5002", "5001"]
	for player in candidates:
		if player not in installed:
			continue
		if player == "1":
			if confirmedTs:
				return player
		elif player in ("4097", "5001", "5002") and scheme in ("http", "https", "rtsp", "rtmp", "udp", "rtp"):
			if kind in ("dash", "hls", "mpeg-ts", "progressive", "radio", "rtsp", "multicast", "multicast-ts"):
				return player
	return ""


# Broadcast matching and native bouquets


def parseNumber(value, base=10):
	if value is None or value == "":
		return None
	if isinstance(value, int):
		return value
	text = str(value).strip()
	try:
		return int(text, 0) if text.lower().startswith("0x") else int(text, base)
	except Exception:
		return None


class ServiceMatcher:
	"""Match by DVB triplet, including the German DVB-T/C wildcard rules."""

	def __init__(self, lamedbPath="/etc/enigma2/lamedb", serviceSnapshot=None, requireFtaVerification=False):
		def loadSnapshot(snapshot):
			def serviceFromReference(reference):
				parts = (reference or "").split(":")
				if len(parts) < 7 or parts[0] != "1":
					return None
				try:
					return {
						"service_type": int(parts[2], 16),
						"sid": int(parts[3], 16),
						"tsid": int(parts[4], 16),
						"onid": int(parts[5], 16),
						"namespace": int(parts[6], 16),
						"ref": ":".join(parts[:10]) + ":",
						"crypted": False,
						"fta_verified": False,
						"name": "",
					}
				except Exception:
					return None

			for value in snapshot:
				verified = isinstance(value, dict) and isinstance(value.get("crypted"), bool)
				if isinstance(value, str):
					value = {"ref": value, "crypted": False}
				if not isinstance(value, dict):
					continue
				parsed = serviceFromReference(value.get("ref", ""))
				if parsed:
					parsed["crypted"] = bool(value.get("crypted", False))
					parsed["fta_verified"] = verified
					parsed["name"] = value.get("name", "")
					parsed["network_id"] = value.get("network_id")
					self.services.append(parsed)
			self.loaded = True

		self.lamedbPath = lamedbPath
		self.services = []
		self.loaded = False
		self.requireFtaVerification = bool(requireFtaVerification)
		if serviceSnapshot is not None:
			loadSnapshot(serviceSnapshot)

	def load(self):
		def loadLamedbV4(lines):
			pattern = reCompile(r"^([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)(?::.*)?$")
			for line in lines:
				match = pattern.match(line.strip())
				if not match:
					continue
				sid, namespace, tsid, onid = [int(match.group(index), 16) for index in range(1, 5)]
				serviceType = int(match.group(5), 10)
				self.services.append(self.makeService(sid, namespace, tsid, onid, serviceType))

		def loadLamedbV5(lines):
			pattern = reCompile(r"^s:([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+):")
			for line in lines:
				match = pattern.match(line.strip())
				if not match:
					continue
				sid, namespace, tsid, onid = [int(match.group(index), 16) for index in range(1, 5)]
				serviceType = int(match.group(5), 10)
				self.services.append(self.makeService(sid, namespace, tsid, onid, serviceType))

		if self.loaded:
			return
		self.loaded = True
		if not pathExists(self.lamedbPath):
			return
		try:
			with open(self.lamedbPath, "r", encoding="utf-8", errors="replace") as handle:
				lines = handle.readlines()
		except Exception:
			return

		version5 = any(line.startswith("eDVB services /5/") for line in lines[:3])
		if version5:
			loadLamedbV5(lines)
		else:
			loadLamedbV4(lines)

	def makeService(self, sid, namespace, tsid, onid, serviceType):
		reference = "1:0:{0:X}:{1:X}:{2:X}:{3:X}:{4:X}:0:0:0:".format(serviceType, sid, tsid, onid, namespace)
		return {
			"sid": sid,
			"tsid": tsid,
			"onid": onid,
			"namespace": namespace,
			"service_type": serviceType,
			"ref": reference,
			"crypted": False,
			"fta_verified": False,
			"name": "",
		}

	def matchService(self, dvbiService, includeDrm=False):
		"""Select the highest-priority installed broadcast instance."""
		self.load()

		def priority(instance):
			value = parseNumber(getattr(instance, "priority", None), 10)
			return value if value is not None else 0x7FFFFFFF

		instances = sorted(dvbiService.instances, key=priority)
		dvbiService.matchedRefs = []
		for instance in instances:
			if not isInstanceAvailable(instance):
				continue
			if getattr(instance, "drm", False) and not includeDrm:
				continue
			match = self.matchInstance(instance, includeDrm=includeDrm)
			if match:
				if not dvbiService.matchedRefs:
					dvbiService.matchedInstance = instance
					dvbiService.matchedCrypted = bool(match.get("crypted", False))
				if match["ref"] not in dvbiService.matchedRefs:
					dvbiService.matchedRefs.append(match["ref"])
		return dvbiService.matchedRefs[0] if dvbiService.matchedRefs else ""

	def matchInstance(self, instance, includeDrm=False):
		sid = parseNumber(getattr(instance, "serviceId", None), 10)
		tsid = parseNumber(getattr(instance, "transportStreamId", None), 10)
		onid = parseNumber(getattr(instance, "originalNetworkId", None), 10)
		namespace = parseNumber(getattr(instance, "namespace", None), 10)
		instanceType = getattr(instance, "instanceType", "")
		networkId = parseNumber(getattr(instance, "networkId", None))
		orbital = getattr(instance, "orbitalPosition", None)
		try:
			position = str(orbital).strip().upper()
			orbital = int(round(float(position.rstrip("EW")) * 10))
			if position.endswith("W"):
				orbital = -orbital
			orbital %= 3600
		except (ValueError, TypeError):
			if orbital not in (None, ""):
				return None
			orbital = None
		if sid is None or instanceType not in ("dvb-s", "dvb-c", "dvb-t", "broadcast"):
			return None

		candidates = []
		for service in self.services:
			position = service["namespace"] >> 16
			delivery = "dvb-t" if position == 0xEEEE else "dvb-c" if position == 0xFFFF else "dvb-s"
			if instanceType not in ("broadcast", delivery):
				continue
			if delivery == "dvb-s" and orbital is not None and position != orbital:
				continue
			# Never silently ignore a cable-network restriction. E2 snapshots
			# without NID cannot safely resolve such a wildcard.
			if networkId is not None and networkId != service.get("network_id"):
				continue
			if service["sid"] != sid:
				continue
			if tsid is not None and service["tsid"] != tsid:
				continue
			if onid is not None and service["onid"] != onid:
				continue
			if namespace is not None and service["namespace"] != namespace:
				continue
			if service.get("crypted", False) and not includeDrm:
				continue
			if self.requireFtaVerification and not includeDrm and not service.get("fta_verified", False):
				continue
			candidates.append(service)

		if not candidates:
			return None

		completeTriplet = tsid is not None and onid is not None
		if completeTriplet:
			uniqueCandidates = {candidate["ref"]: candidate for candidate in candidates}
			return next(iter(uniqueCandidates.values())) if len(uniqueCandidates) == 1 else None

		# DVB-I permits ONID+SID for terrestrial and SID-only for cable.
		wildcardAllowed = instanceType == "dvb-t" and onid is not None or instanceType == "dvb-c" and tsid is None and onid is None
		if not wildcardAllowed:
			return None

		uniqueCandidates = {}
		for candidate in candidates:
			uniqueCandidates[candidate["ref"]] = candidate
		return next(iter(uniqueCandidates.values())) if len(uniqueCandidates) == 1 else None


DVB_I_MEDIA_HINT_DASH = 0x100
DVB_I_MEDIA_HINT_HLS = 0x200


def bouquetSlug(value):
	value = value or "dvbi"
	value = value.lower()
	value = reSub(r"[^a-z0-9]+", "_", value)
	value = reSub(r"_+", "_", value).strip("_")
	return value or "dvbi"


def cleanDescription(value):
	return (value or "Unnamed").replace("\n", " ").replace("\r", " ").strip()


def hexFromDigest(digest, start, length, minimum=1):
	value = int(digest[start: start + length], 16)
	return max(value, minimum)


def enigmaHex(value, width=0):
	if width:
		return ("%0" + str(width) + "X") % int(value)
	return "%X" % int(value)


def serviceHasIp(service):
	return any(instance.instanceType in ("dash", "hls", "ip", "radio", "identifier", "rtsp", "multicast") and instance.url for instance in service.instances)


def serviceHasDrm(service):
	return any(instance.drm for instance in service.instances)


def serviceHasHbbtv(service):
	return any(instance.hbbtv for instance in service.instances)


def regionIds(value):
	if isinstance(value, (list, tuple, set)):
		values = value
	else:
		values = reSplit(r"[,;]", value or "")
	return set(str(item).strip().casefold() for item in values if str(item).strip())


def serviceVisibilityStatus(service, requestedRegion="", showOtherRegions=True):
	if not getattr(service, "lcnSelectable", True):
		return "skipped_lcn_unselectable"
	if not getattr(service, "lcnVisible", True):
		return "skipped_lcn_hidden"
	requested = regionIds(requestedRegion)
	serviceRegions = regionIds(getattr(service, "regions", []))
	if not showOtherRegions and requested and serviceRegions and requested.isdisjoint(serviceRegions):
		return "skipped_other_region"
	return ""


class BouquetWriter:
	"""Write DVB-I bouquets without changing Enigma2 core data structures."""

	def __init__(self, enigma2Dir="/etc/enigma2"):
		self.enigma2Dir = enigma2Dir
		self.availablePlayers = ()
		self.automaticPlayer = True

	def write(self, serviceList, bouquetKey=None, includeIp=True, includeDrm=False, ipServiceType="4097", preferBroadcast=True, reloadBouquets=True, commit=True, requireFtaVerification=False, showOtherRegions=True, availablePlayers=(), automaticPlayer=True, hybrid=False, labelVod=False):
		"""Create or update separate DVB-I TV and radio bouquets."""

		def selectRef(service, includeIp, includeDrm, ipServiceType, preferBroadcast, requireFtaVerification=False):
			"""Select the Enigma2 service reference for one DVB-I service."""
			matchedInstance = getattr(service, "matchedInstance", None)
			matchedIsProtected = bool(getattr(service, "matchedCrypted", False) or matchedInstance is not None and getattr(matchedInstance, "drm", False))
			canUseBroadcast = bool(
				preferBroadcast
				and service.matchedRef
				and (matchedInstance is None or isInstanceAvailable(matchedInstance))
				and (includeDrm or not matchedIsProtected)
			)

			if preferBroadcast and canUseBroadcast:
				service.selectedRef = service.matchedRef
				service.selectedInstanceType = "broadcast"
				service.selectedInstance = matchedInstance
				service.selectedPlayerType = str(service.matchedRef).split(":", 1)[0] or "1"
				service.selectedMediaKind = "broadcast"
				service.status = "matched_broadcast"
				return service.matchedRef

			ipRef = self.ipRef(service, includeIp, includeDrm, ipServiceType, requireFtaVerification)
			if ipRef:
				return ipRef

			if serviceHasHbbtv(service) and not serviceHasIp(service):
				service.status = "hbbtv_required"
			elif not service.status or service.status in ("not_checked", "broadcast_match_available", "matched_broadcast"):
				service.status = "skipped_no_supported_instance"

			return ""

		self.availablePlayers = availablePlayers
		self.automaticPlayer = automaticPlayer
		if commit and not pathExists(self.enigma2Dir):
			makedirs(self.enigma2Dir)

		bouquetKey = bouquetKey or bouquetSlug(serviceList.country + "_" + serviceList.region + "_" + serviceList.name)
		bouquetBase = "userbouquet.dvbi_{0}".format(bouquetSlug(bouquetKey))
		title = "DVB-I {0}".format(serviceList.name or serviceList.country or "Services")
		if not preferBroadcast:
			title += " (IP)"
		artifacts = {}
		for kind in ("tv", "radio"):
			bouquetName = "{0}.{1}".format(bouquetBase, kind)
			artifacts[kind] = {
				"kind": kind,
				"bouquet": bouquetName,
				"path": join(self.enigma2Dir, bouquetName),
				"title": title if kind == "tv" else title + " Radio",
				"lines": [],
				"references": [],
				"added": 0,
				"skipped": 0,
				"broadcast_written": 0,
				"ip_written": 0,
			}
			artifacts[kind]["lines"].append("#NAME {0}\n".format(cleanDescription(artifacts[kind]["title"])))

		stats = {
			"bouquet_tv": artifacts["tv"]["bouquet"],
			"bouquet_tv_path": artifacts["tv"]["path"],
			"bouquet_radio": artifacts["radio"]["bouquet"],
			"bouquet_radio_path": artifacts["radio"]["path"],
			"added": 0,
			"skipped": 0,
			"services_written_tv": 0,
			"services_written_radio": 0,
			"broadcast_written": 0,
			"ip_written": 0,
			"drm_marked": 0,
			"hbbtv_marked": 0,
			"no_supported_instance": 0,
			"unchanged": False,
			"bouquets_tv_changed": False,
			"bouquets_radio_changed": False,
			"prefer_broadcast": bool(preferBroadcast),
			"ip_service_type": str(ipServiceType),
			"services": [],
			"hybrid_services": [],
			"bouquets": {},
		}

		services = list(serviceList.services)
		services.sort(key=lambda item: (item.lcn is None, item.lcn or 999999, item.name.lower()))

		for service in services:
			service.playbackScope = getattr(serviceList, "listId", "") or serviceList.sourceUrl
			# Matching may have provisionally selected a broadcast reference.
			# Only a service actually materialised below may keep a selection;
			# otherwise logo, EPG and playback-map stages could publish a
			# hidden/data service accidentally.
			service.selectedRef = ""
			service.selectedInstance = None
			service.selectedInstanceType = ""
			service.selectedPlayerType = ""
			service.selectedMediaKind = ""
			service.playbackToken = ""
			service.hybridRefs = []
			serviceKind, unusedEvidence = classifyService(service)
			if serviceKind not in artifacts:
				service.status = "skipped_service_type_{0}".format(serviceKind)
				stats["skipped"] += 1
				stats["services"].append(self.serviceReportItem(service, False))
				continue
			artifact = artifacts[serviceKind]

			if serviceHasDrm(service):
				stats["drm_marked"] += 1
			if serviceHasHbbtv(service):
				stats["hbbtv_marked"] += 1

			visibilityStatus = serviceVisibilityStatus(service, serviceList.region, showOtherRegions)
			if visibilityStatus:
				service.status = visibilityStatus
				stats["skipped"] += 1
				artifact["skipped"] += 1
				stats["services"].append(self.serviceReportItem(service, False))
				continue

			ref = selectRef(service, includeIp, includeDrm, ipServiceType, preferBroadcast, requireFtaVerification)

			if not ref:
				if not service.status or service.status in ("not_checked", "unknown"):
					service.status = "skipped_no_supported_instance"
				if service.status == "skipped_no_supported_instance":
					stats["no_supported_instance"] += 1
				stats["skipped"] += 1
				artifact["skipped"] += 1
				stats["services"].append(self.serviceReportItem(service, False))
				continue

			service.selectedRef = ref
			if hybrid and service.matchedRef and not includeDrm and requireFtaVerification:
				# Work on a copy: the logical channel and its EPG remain DVB.
				alternative = copy(service)
				ipRef = self.ipRef(alternative, includeIp, False, ipServiceType, True)
				if ipRef and alternative.selectedInstance.url.startswith(("http://", "https://")):
					ipFields = ipRef.split(":", 11)
					for broadcast in getattr(service, "matchedRefs", None) or [service.matchedRef]:
						fields = broadcast.split(":")[:10]
						fields[0], fields[9] = ipFields[0], ipFields[9]
						target = ":".join(fields + [quote(playbackUrl(alternative.selectedInstance), safe="/"), ipFields[11]])
						service.hybridRefs.append(target)
						stats["hybrid_services"].append((broadcast, target))
			name = cleanDescription(service.name)
			if (
				labelVod
				and service.selectedInstanceType != "broadcast"
				and (service.serviceTypeTerm in ("ondemand", "ondemand-radio") or getattr(service.selectedInstance, "manifestVod", False))
			):
				name += " [VOD]"
			artifact["lines"].append("#SERVICE {0}\n".format(ref))
			description = name
			if service.provider:
				description += "•" + cleanDescription(service.provider)
			artifact["lines"].append("#DESCRIPTION {0}\n".format(description))
			artifact["references"].append({"reference": ref, "name": description})
			stats["added"] += 1
			artifact["added"] += 1

			if service.selectedInstanceType == "broadcast":
				stats["broadcast_written"] += 1
				artifact["broadcast_written"] += 1
			elif service.selectedInstanceType in ("dash", "hls", "ip", "radio", "identifier", "rtsp", "multicast"):
				stats["ip_written"] += 1
				artifact["ip_written"] += 1

			stats["services"].append(self.serviceReportItem(service, True))

		for kind in ("tv", "radio"):
			artifact = artifacts[kind]
			content = "".join(artifact.pop("lines"))
			exists = pathExists(artifact["path"])
			oldContent = ""
			if exists:
				try:
					with open(artifact["path"], "r", encoding="utf-8", errors="replace") as handle:
						oldContent = handle.read()
				except Exception:
					oldContent = ""
			artifact["active"] = artifact["added"] > 0
			artifact["unchanged"] = oldContent == content if artifact["active"] else not exists
			artifact["content"] = content
			stats["bouquets"][kind] = {key: value for key, value in artifact.items() if key not in ("content", "references")}

		stats["services_written_tv"] = artifacts["tv"]["added"]
		stats["services_written_radio"] = artifacts["radio"]["added"]
		stats["unchanged"] = all(artifact["unchanged"] for artifact in artifacts.values())
		stats["_artifacts"] = artifacts
		stats["_bouquet_contents"] = {kind: artifact["content"] for kind, artifact in artifacts.items()}
		stats["_reload_bouquets"] = bool(reloadBouquets)
		return self.commitPrepared(stats) if commit else stats

	def nativePayload(self, stats):
		"""Export a complete list; only eDVBDB may publish it on the receiver."""
		artifacts = stats.pop("_artifacts")
		for key in ("_bouquet_contents", "_reload_bouquets"):
			stats.pop(key, None)
		return [
			{
				"filename": artifact["bouquet"],
				"name": cleanDescription(artifact["title"]),
				"services": artifact["references"],
			}
			for artifact in artifacts.values()
		]

	def commitPrepared(self, stats):
		"""Publish both child bouquets, then roots, and reload exactly once."""

		def ensureBouquetRegistered(bouquetName, kind="tv", enabled=True):
			kind = "radio" if kind == "radio" else "tv"
			path = join(self.enigma2Dir, "bouquets.{0}".format(kind))
			rootType = "2" if kind == "radio" else "1"
			entry = '#SERVICE 1:7:{0}:0:0:0:0:0:0:0:FROM BOUQUET "{1}" ORDER BY bouquet\n'.format(rootType, bouquetName)

			lines = []
			if pathExists(path):
				with open(path, "r", encoding="utf-8", errors="replace") as handle:
					lines = handle.readlines()

			if not lines and not enabled:
				return False

			original = list(lines)
			marker = 'FROM BOUQUET "{0}"'.format(bouquetName)
			matching = [index for index, line in enumerate(lines) if marker in line]
			if enabled:
				if matching:
					first = matching[0]
					lines[first] = entry
					matching = set(matching[1:])
					lines = [line for index, line in enumerate(lines) if index not in matching]
				else:
					if not lines:
						title = "Radio" if kind == "radio" else "TV"
						lines = ["#NAME User - bouquets ({0})\n".format(title)]
					lines.append(entry)
			elif matching:
				matching = set(matching)
				lines = [line for index, line in enumerate(lines) if index not in matching]
			changed = lines != original

			if changed:
				atomicWrite(path, "".join(lines))

			return changed

		artifacts = stats.pop("_artifacts")
		stats.pop("_bouquet_contents", None)
		reloadBouquets = stats.pop("_reload_bouquets", False)

		# Active children are visible before their root references are added.
		for artifact in artifacts.values():
			if artifact["active"] and not artifact["unchanged"]:
				atomicWrite(artifact["path"], artifact["content"])

		rootChanges = {}
		for kind in ("tv", "radio"):
			artifact = artifacts[kind]
			rootChanges[kind] = ensureBouquetRegistered(artifact["bouquet"], kind, enabled=artifact["active"])

		# Remove an obsolete plugin-owned child only after it is hidden from
		# the corresponding root bouquet.
		for artifact in artifacts.values():
			if not artifact["active"] and pathExists(artifact["path"]):
				unlink(artifact["path"])

		stats["bouquets_tv_changed"] = rootChanges["tv"]
		stats["bouquets_radio_changed"] = rootChanges["radio"]
		for kind, artifact in artifacts.items():
			stats["bouquets"][kind]["root_changed"] = rootChanges[kind]
			stats["bouquets"][kind]["file_changed"] = not artifact["unchanged"]
		if reloadBouquets and (not stats.get("unchanged", False) or stats["bouquets_tv_changed"] or stats["bouquets_radio_changed"]):
			self.reloadBouquets()
		return stats

	def ipRef(self, service, includeIp, includeDrm, ipServiceType, requireFtaVerification=False):
		def uniqueIpIds(service, instance):
			"""Generate stable non-zero pseudo DVB identifiers for IPTV/DASH services.

			The identity deliberately excludes CDN URLs. All delivery instances of
			one DVB-I service therefore share the same EPG/picon triplet.
			"""
			key = "|".join(
				[
					service.dvbiId or "",
					service.name or "",
					service.provider or "",
					"dvbi-v2",
				]
			)
			digest = sha1(key.encode("utf-8")).hexdigest()
			sid = hexFromDigest(digest, 0, 4, 1)
			tsid = hexFromDigest(digest, 4, 4, 1)
			onid = 0xD100 | (hexFromDigest(digest, 8, 2, 1) & 0x00FF)
			namespace = 0xD1B10000 | hexFromDigest(digest, 10, 4, 1)
			return sid, tsid, onid, namespace

		def stableToken(service):
			serviceIdentity = service.dvbiId or "|".join([service.country, service.provider, service.name])
			identity = "|".join([getattr(service, "playbackScope", ""), serviceIdentity])
			return sha256(identity.encode("utf-8")).hexdigest()[:32]

		if not includeIp:
			return ""

		skippedDrm = False
		skippedUnverified = False
		skippedHbbtv = False
		skippedUnavailable = False
		skippedPlayer = False

		def priority(instance):
			try:
				return int(getattr(instance, "priority", None))
			except Exception:
				return 0x7FFFFFFF

		for instance in sorted(service.instances, key=priority):
			if instance.instanceType not in ("dash", "hls", "ip", "radio", "identifier", "rtsp", "multicast"):
				continue
			if not isInstanceAvailable(instance) or getattr(instance, "playbackUnavailable", False):
				skippedUnavailable = True
				continue
			if instance.drm and not includeDrm:
				skippedDrm = True
				continue
			if requireFtaVerification and not includeDrm and not getattr(instance, "ftaVerified", False):
				skippedUnverified = True
				continue
			if not instance.url:
				if instance.hbbtv:
					skippedHbbtv = True
				continue

			playerType = selectPlayer(instance, ipServiceType, self.availablePlayers, self.automaticPlayer)
			if not playerType:
				skippedPlayer = True
				continue
			token = stableToken(service)
			referenceUrl = playbackUrl(instance) if playerType == "1" else "dvbi://{0}".format(token)
			encoded = quote(referenceUrl, safe="/")
			referenceName = cleanDescription(service.name)
			if service.provider:
				referenceName += "•" + cleanDescription(service.provider)
			description = quote(referenceName, safe="")
			sid, tsid, onid, namespace = uniqueIpIds(service, instance)
			service.selectedInstanceType = instance.instanceType
			service.selectedInstance = instance
			service.selectedPlayerType = playerType
			service.selectedMediaKind = effectiveMediaKind(instance)
			service.playbackToken = token
			if instance.drm:
				service.status = "drm_included"
			elif instance.hbbtv:
				service.status = "playable_unchecked_hbbtv_marker"
			else:
				service.status = "playable_unchecked"
			mediaHint = self.mediaHint(instance, playerType)
			serviceType = "2" if getattr(service, "mediaKind", "tv") == "radio" else "1"
			ref = "{0}:0:{1}:{2}:{3}:{4}:{5}:0:0:{6}:{7}:{8}".format(
				playerType,
				serviceType,
				enigmaHex(sid),
				enigmaHex(tsid),
				enigmaHex(onid),
				enigmaHex(namespace, 8),
				enigmaHex(mediaHint),
				encoded,
				description,
			)
			service.selectedRef = ref
			return ref

		if skippedDrm:
			service.status = "drm_required"
		elif skippedUnverified:
			service.status = "skipped_unverified_fta"
		elif skippedUnavailable:
			service.status = "not_currently_available"
		elif skippedHbbtv:
			service.status = "hbbtv_required"
		elif skippedPlayer:
			service.status = "skipped_no_compatible_player"

		return ""

	def mediaHint(self, instance, playerType):
		if str(playerType) != "4097":
			return 0
		kind = effectiveMediaKind(instance)
		if kind == "dash":
			return DVB_I_MEDIA_HINT_DASH
		if kind == "hls":
			return DVB_I_MEDIA_HINT_HLS
		return 0

	def serviceReportItem(self, service, written):
		return {
			"name": service.name,
			"provider": service.provider,
			"lcn": service.lcn,
			"written": bool(written),
			"status": service.status,
			"selected_instance_type": service.selectedInstanceType,
			"has_broadcast_match": bool(service.matchedRef),
			"has_ip": serviceHasIp(service),
			"has_drm": serviceHasDrm(service),
			"has_hbbtv": serviceHasHbbtv(service),
			"selected_ref": service.selectedRef,
			"selected_player_type": getattr(service, "selectedPlayerType", ""),
			"selected_media_kind": getattr(service, "selectedMediaKind", ""),
			"service_type_uri": getattr(service, "serviceTypeUri", ""),
			"service_type_term": getattr(service, "serviceTypeTerm", ""),
			"service_media_kind": getattr(service, "mediaKind", "tv"),
			"service_media_kind_source": getattr(service, "mediaKindSource", "default_tv"),
			"service_kind": getattr(service, "mediaKind", "tv"),
			"service_kind_source": getattr(service, "mediaKindSource", "default_tv"),
			"selected_priority": getattr(getattr(service, "selectedInstance", None), "priority", None),
		}

	def reloadBouquets(self):
		from enigma import eDVBDB

		database = eDVBDB.getInstance()
		database.reloadServicelist()
		database.reloadBouquets()


def installBouquets(payload, database, referenceFactory, serviceCenter=None):
	if not isinstance(payload, list) or len(payload) != 2:
		raise ValueError(_("Incomplete DVB-I TV/radio channel-list result"))
	prepared = []
	filenames = set()
	for group in payload:
		filename = group.get("filename", "")
		if not reFullmatch(r"userbouquet\.dvbi_[a-z0-9_]+\.(tv|radio)", filename) or filename in filenames:
			raise ValueError(_("Invalid DVB-I channel-list filename"))
		filenames.add(filename)
		name = group.get("name", "")
		if not isinstance(name, str) or any(char in name for char in "\r\n\x00"):
			raise ValueError(_("Invalid DVB-I channel-list name"))
		services = group.get("services")
		if not isinstance(services, list) or len(services) > 10000:
			raise ValueError(_("Invalid DVB-I channel-list size"))
		references = []
		for item in services:
			value, label = item.get("reference", ""), item.get("name", "")
			if not isinstance(value, str) or not isinstance(label, str) or any(char in value + label for char in "\r\n\x00"):
				raise ValueError(_("Invalid DVB-I service reference"))
			ref = referenceFactory(value)
			if not ref.valid():
				raise ValueError(_("Invalid DVB-I service reference"))
			# eDVBDB writes #DESCRIPTION from ref.name; URL-quoted reference
			# labels must not leak into the native service list.
			ref.setName(label)
			references.append(ref.toString())
		prepared.append((name, filename, references))
	if len({name.rsplit(".", 1)[0] for name in filenames}) != 1:
		raise ValueError(_("DVB-I TV/radio lists do not belong together"))
	if not any(references for unusedName, unusedFile, references in prepared):
		raise ValueError(_("No playable free-to-air channels found. Existing channel lists were not changed."))
	if serviceCenter is None:
		from enigma import eServiceCenter

		serviceCenter = eServiceCenter.getInstance()
	# Validate the complete result before the first native write. Normal
	# bouquets and other imported DVB-I lists are never removed or reordered.
	for name, filename, references in prepared:
		if not references:
			continue
		if database.addOrUpdateBouquet(name, filename, references, False) != 0:
			raise RuntimeError(_("Enigma2 could not save the DVB-I channel list: ") + filename)
		# addOrUpdateBouquet only sets the title when creating a new bouquet.
		# Rename through the same native editable-list API as ChannelSelection.
		kind = 2 if filename.endswith(".radio") else 1
		ref = referenceFactory('1:7:{0}:0:0:0:0:0:0:0:FROM BOUQUET "{1}" ORDER BY bouquet'.format(kind, filename))
		listing = serviceCenter.list(ref) if serviceCenter is not None else None
		editable = listing.startEdit() if listing is not None else None
		if editable is None or editable.setListName(name) != 0 or editable.flushChanges() != 0:
			raise RuntimeError(_("Enigma2 could not name the DVB-I channel list: ") + filename)
	for unusedName, filename, references in prepared:
		if not references and database.removeBouquet("^" + reEscape(filename) + "$") != 0:
			raise RuntimeError(_("Enigma2 could not remove an empty DVB-I channel list: ") + filename)
	database.reloadBouquets()


def getAvailablePlayers():
	from enigma import eServiceCenter, eServiceReference

	center = eServiceCenter.getInstance()
	result = []
	for player in ("1", "4097", "5001", "5002"):
		# Query static information only. Never start a decoder or stream.
		ref = eServiceReference(player + ":0:1:0:0:0:0:0:0:0:http%3a//127.0.0.1/dvbi-probe:Capability")
		if center.info(ref) is not None:
			binaries = {"5001": ("/usr/bin/gstplayer2", "/usr/bin/gstplayer"), "5002": ("/usr/bin/exteplayer3",)}.get(player, ())
			if not binaries or any(access(binary, X_OK) for binary in binaries):
				result.append(player)
	return result


def refreshPicons():
	from Components.Renderer.Picon import resetPiconPath
	from Components.Renderer.LcdPicon import resetLcdPiconPath

	resetPiconPath()
	resetLcdPiconPath()


# ContentGuide and EPG synchronization


QUERY_BOUNDARY_SECONDS = 3 * 60 * 60
QUERY_DURATIONS = (6 * 60 * 60, 12 * 60 * 60)
XML_LANGUAGE = "{http://www.w3.org/XML/1998/namespace}lang"
MAX_XML_BYTES = 32 * 1024 * 1024

DURATION_RE = reCompile(
	r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?"
	r"(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:[\.,]\d+)?)S)?)?$"
)
CONTENT_SUBJECT_RE = reCompile(
	r"^urn:dvb:metadata:cs:ContentSubject:(?:\d{4}):(?P<main>\d+)"
	r"(?:[\.:](?P<sub>\d+))?$",
	IGNORECASE,
)
STANDARD_QUERY_KEYS = frozenset(("sid", "start", "end", "now_next", "inclusive", "image_variant"))


def guideLocalName(tag):
	"""Return an XML local name without relying on a TVA namespace version."""
	if not isinstance(tag, str):
		return ""
	return tag.rsplit("}", 1)[-1].split(":", 1)[-1]


def elementText(element):
	if element is None:
		return ""
	return " ".join("".join(element.itertext()).split())


def guideFirstDescendant(element, name):
	if element is None:
		return None
	for child in element.iter():
		if child is not element and guideLocalName(child.tag) == name:
			return child
	return None


def descendants(element, name):
	if element is None:
		return []
	return [child for child in element.iter() if child is not element and guideLocalName(child.tag) == name]


def normaliseSid(contentGuideServiceRef):
	sid = str(contentGuideServiceRef or "").strip()
	if not sid:
		raise ValueError(_("ContentGuideServiceRef is required as the sid parameter"))
	return sid


def normaliseImageVariants(imageVariants):
	if imageVariants is None or imageVariants == "":
		return []
	if isinstance(imageVariants, str):
		imageVariants = [imageVariants]
	result = []
	for value in imageVariants:
		value = str(value).strip()
		if value:
			result.append(value)
	return result


def urlWithQuery(endpoint, query):
	def validateEndpoint(endpoint):
		parsed = urlsplit(endpoint or "")
		if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
			raise ValueError(_("ScheduleInfoEndpoint must use HTTP or HTTPS"))
		if parsed.username is not None or parsed.password is not None:
			raise ValueError(_("credentials in ScheduleInfoEndpoint URLs are not supported"))
		return parsed

	parsed = validateEndpoint(endpoint)
	# Preserve provider-specific authentication/routing parameters but replace
	# any stale DVB-I schedule parameters already present on the endpoint.
	existing = [(key, value) for key, value in parseQsl(parsed.query, keep_blank_values=True) if key.lower() not in STANDARD_QUERY_KEYS]
	return urlunsplit(
		(
			parsed.scheme,
			parsed.netloc,
			parsed.path,
			urlencode(existing + query, doseq=True),
			parsed.fragment,
		)
	)


def buildNowNextUrl(endpoint, contentGuideServiceRef, imageVariants=None):
	"""Build a DVB-I now/next request using ContentGuideServiceRef as ``sid``."""
	query = [
		("sid", normaliseSid(contentGuideServiceRef)),
		("now_next", "true"),
	]
	for variant in normaliseImageVariants(imageVariants):
		query.append(("image_variant", variant))
	return urlWithQuery(endpoint, query)


def validateScheduleWindow(start, end):
	"""Validate the exact DVB-I six/twelve-hour timestamp query rules."""
	if isinstance(start, bool) or isinstance(end, bool):
		raise ValueError(_("schedule timestamps must be Unix integers"))
	try:
		start = int(start)
		end = int(end)
	except (TypeError, ValueError) as error:
		raise ValueError(_("schedule timestamps must be Unix integers")) from error
	if start < 0 or end < 0:
		raise ValueError(_("schedule timestamps must not be negative"))
	if start % QUERY_BOUNDARY_SECONDS or end % QUERY_BOUNDARY_SECONDS:
		raise ValueError(_("schedule timestamps must be multiples of 10800 seconds"))
	if end - start not in QUERY_DURATIONS:
		raise ValueError(_("schedule windows must be exactly 6 or 12 hours"))
	return start, end


def buildScheduleUrl(endpoint, contentGuideServiceRef, start, end, inclusive=False, imageVariants=None):
	"""Build a timestamp-based DVB-I ScheduleInfoEndpoint request."""
	start, end = validateScheduleWindow(start, end)
	query = [
		("sid", normaliseSid(contentGuideServiceRef)),
		("start", str(start)),
		("end", str(end)),
	]
	if inclusive:
		query.append(("inclusive", "true"))
	for variant in normaliseImageVariants(imageVariants):
		query.append(("image_variant", variant))
	return urlWithQuery(endpoint, query)


def alignedScheduleWindows(start, end, duration=6 * 60 * 60):
	"""Return exact aligned query windows covering the half-open interval.

	``start`` is rounded down and ``end`` is rounded up to a three-hour DVB-I
	boundary.  The resulting requests are still always exactly six or twelve
	hours long, so the final request can extend beyond the requested interval.
	"""
	if duration not in QUERY_DURATIONS:
		raise ValueError(_("window duration must be 6 or 12 hours"))
	try:
		start = int(start)
		end = int(end)
	except (TypeError, ValueError) as error:
		raise ValueError(_("schedule timestamps must be Unix integers")) from error
	if start < 0 or end <= start:
		raise ValueError(_("end must be later than a non-negative start"))
	cursor = (start // QUERY_BOUNDARY_SECONDS) * QUERY_BOUNDARY_SECONDS
	boundaryEnd = ((end + QUERY_BOUNDARY_SECONDS - 1) // QUERY_BOUNDARY_SECONDS) * QUERY_BOUNDARY_SECONDS
	windows = []
	while cursor < boundaryEnd:
		windows.append((cursor, cursor + duration))
		cursor += duration
	return windows


def parseDatetime(value):
	value = (value or "").strip()
	if not value:
		raise ValueError(_("empty dateTime"))
	if value.endswith(("Z", "z")):
		value = value[:-1] + "+00:00"
	parsed = datetime.fromisoformat(value)
	if parsed.tzinfo is None:
		# Real-world test feeds occasionally omit the zone.  Treating these as
		# UTC is deterministic and avoids dependence on the receiver timezone.
		parsed = parsed.replace(tzinfo=timezone.utc)
	return int(parsed.timestamp())


def pickText(elements, preferredLanguage, defaultLanguage, requiredType=None, requiredLength=None):
	def languageScore(language, preferredLanguage, defaultLanguage):
		language = (language or defaultLanguage or "").lower().replace("_", "-")
		preferred = (preferredLanguage or "").lower().replace("_", "-")
		if preferred and language == preferred:
			return 0
		if preferred and language and language.split("-", 1)[0] == preferred.split("-", 1)[0]:
			return 1
		if not language:
			return 2
		if defaultLanguage and language == defaultLanguage.lower().replace("_", "-"):
			return 3
		return 4

	candidates = []
	for index, element in enumerate(elements):
		if requiredType and (element.get("type") or "").lower() != requiredType:
			continue
		if requiredLength and (element.get("length") or "").lower() != requiredLength:
			continue
		value = elementText(element)
		if value:
			candidates.append(
				(
					languageScore(element.get(XML_LANGUAGE), preferredLanguage, defaultLanguage),
					index,
					value,
				)
			)
	return min(candidates)[2] if candidates else ""


def eventToE2Tuple(event):
	"""Convert an event dictionary to ``eEPGCache.importEvents`` format."""
	result = (
		int(event["start"]),
		int(event["duration"]),
		str(event.get("title", "")),
		str(event.get("short", "")),
		str(event.get("long", "")),
		tuple(int(item) for item in event.get("event_types", [])),
	)
	if "event_id" in event and event["event_id"] is not None:
		result += (int(event["event_id"]),)
	return result


class ContentGuideParser:
	"""Namespace-version-tolerant TV-Anytime schedule parser."""

	def parse(self, payload, preferredLanguage="", contentGuideServiceRef=""):
		def parseEvent(element, programmes, serviceId):
			def parseDuration(value):
				match = DURATION_RE.match((value or "").strip())
				if not match:
					raise ValueError(_("unsupported ISO 8601 duration"))
				parts = match.groupdict(default="0")
				seconds = int(parts["days"]) * 86400 + int(parts["hours"]) * 3600 + int(parts["minutes"]) * 60 + float(parts["seconds"].replace(",", "."))
				return int(round(seconds))

			program = guideFirstDescendant(element, "Program")
			programId = (program.get("crid") or "").strip() if program is not None else ""
			startElement = guideFirstDescendant(element, "PublishedStartTime")
			durationElement = guideFirstDescendant(element, "PublishedDuration")
			endElement = guideFirstDescendant(element, "PublishedEndTime")
			try:
				start = parseDatetime(elementText(startElement))
				if durationElement is not None:
					duration = parseDuration(elementText(durationElement))
				elif endElement is not None:
					duration = parseDatetime(elementText(endElement)) - start
				else:
					return None
			except (TypeError, ValueError, OverflowError):
				return None
			if duration <= 0:
				return None

			metadata = programmes.get(programId, {})
			event = {
				"start": start,
				"duration": duration,
				"title": metadata.get("title", ""),
				"short": metadata.get("short", ""),
				"long": metadata.get("long", ""),
				"event_types": list(metadata.get("event_types", [])),
				"program_id": programId,
				"service_id": serviceId,
			}
			for attribute in ("eventId", "eventID", "EventId", "EventID"):
				value = element.get(attribute)
				if value is None:
					continue
				try:
					eventId = int(value, 0)
				except ValueError:
					break
				if 0 <= eventId <= 0xFFFF:
					event["event_id"] = eventId
				break
			return event

		def parseProgramme(element, preferredLanguage, rootLanguage):
			def genreEventType(href):
				"""Map DVB ContentSubject top levels to DVB EIT content bytes."""
				match = CONTENT_SUBJECT_RE.match((href or "").strip())
				if not match:
					return None
				main = int(match.group("main"))
				sub = int(match.group("sub") or 0)
				if main == 12:
					# Adult is outside the EIT 1..11 categories; 0xf is user defined.
					main = 15
				if not 1 <= main <= 15 or not 0 <= sub <= 15:
					return None
				return (main << 4) | sub

			description = guideFirstDescendant(element, "BasicDescription")
			if description is None:
				description = element
			defaultLanguage = element.get(XML_LANGUAGE) or rootLanguage

			titles = descendants(description, "Title")
			title = pickText(titles, preferredLanguage, defaultLanguage, requiredType="main")
			if not title:
				title = pickText(titles, preferredLanguage, defaultLanguage)

			synopses = descendants(description, "Synopsis")
			shortText = pickText(synopses, preferredLanguage, defaultLanguage, requiredLength="short")
			longText = pickText(synopses, preferredLanguage, defaultLanguage, requiredLength="long")
			if not longText:
				longText = pickText(synopses, preferredLanguage, defaultLanguage, requiredLength="medium")
			if not shortText and not longText:
				shortText = pickText(synopses, preferredLanguage, defaultLanguage)

			eventTypes = []
			for genre in descendants(description, "Genre"):
				eventType = genreEventType(genre.get("href"))
				if eventType is not None and eventType not in eventTypes:
					eventTypes.append(eventType)
			return {
				"title": title,
				"short": shortText,
				"long": longText,
				"event_types": eventTypes,
			}

		if not isinstance(payload, (bytes, str)):
			raise TypeError(_("Content Guide XML must be bytes or text"))
		length = len(payload.encode("utf-8")) if isinstance(payload, str) else len(payload)
		if length > MAX_XML_BYTES:
			raise ValueError(_("Content Guide XML exceeds the size limit"))
		marker = payload.upper() if isinstance(payload, bytes) else payload.upper()
		doctype = b"<!DOCTYPE" if isinstance(marker, bytes) else "<!DOCTYPE"
		entity = b"<!ENTITY" if isinstance(marker, bytes) else "<!ENTITY"
		if doctype in marker or entity in marker:
			raise ValueError(_("DTD and entity declarations are not supported"))

		root = fromstring(payload)
		if guideLocalName(root.tag) != "TVAMain":
			raise ValueError(_("Content Guide root must be TVAMain"))
		rootLanguage = root.get(XML_LANGUAGE, "")

		programmes = {}
		for element in root.iter():
			if guideLocalName(element.tag) != "ProgramInformation":
				continue
			programId = (element.get("programId") or "").strip()
			if programId:
				programmes[programId] = parseProgramme(element, preferredLanguage, rootLanguage)

		requestedSid = (contentGuideServiceRef or "").strip()
		events = []
		seen = set()
		for schedule in root.iter():
			if guideLocalName(schedule.tag) != "Schedule":
				continue
			serviceIds = (schedule.get("serviceIDRef") or "").split()
			if requestedSid and serviceIds and requestedSid not in serviceIds:
				continue
			serviceId = requestedSid or (serviceIds[0] if serviceIds else "")
			for scheduleEvent in [child for child in list(schedule) if guideLocalName(child.tag) == "ScheduleEvent"]:
				event = parseEvent(scheduleEvent, programmes, serviceId)
				if event is None:
					continue
				key = (event.get("service_id", ""), event["start"], event.get("program_id", ""))
				if key not in seen:
					events.append(event)
					seen.add(key)

		events.sort(key=lambda item: (item["start"], item.get("service_id", ""), item.get("program_id", "")))
		return events

	def parseE2(self, payload, preferredLanguage="", contentGuideServiceRef=""):
		return [eventToE2Tuple(event) for event in self.parse(payload, preferredLanguage, contentGuideServiceRef)]


class ContentGuideResponse:
	"""Parsed response and cache metadata for the background import."""

	def __init__(self, url, contentGuideServiceRef, events, fetchResult):
		self.url = url
		self.contentGuideServiceRef = contentGuideServiceRef
		self.events = events
		self.fetchResult = fetchResult

	@property
	def e2Events(self):
		return [eventToE2Tuple(event) for event in self.events]


class ContentGuideClient:
	"""Fetch and parse ScheduleInfoEndpoint responses off the Enigma2 UI path."""

	def __init__(self, cacheDir=None, fetcher=None, parser=None):
		if fetcher is None:
			if not cacheDir:
				raise ValueError(_("cache_dir is required when no fetcher is supplied"))
			fetcher = ServiceListFetcher(cacheDir)
		self.fetcher = fetcher
		self.parser = parser or ContentGuideParser()

	def fetchNowNext(self, endpoint, contentGuideServiceRef, preferredLanguage="", imageVariants=None, timeout=20, force=False):
		sid = normaliseSid(contentGuideServiceRef)
		url = buildNowNextUrl(endpoint, sid, imageVariants=imageVariants)
		return self.fetch(url, sid, preferredLanguage, timeout, force)

	def fetchSchedule(self, endpoint, contentGuideServiceRef, start, end, preferredLanguage="", inclusive=False, imageVariants=None, timeout=20, force=False):
		sid = normaliseSid(contentGuideServiceRef)
		url = buildScheduleUrl(
			endpoint,
			sid,
			start,
			end,
			inclusive=inclusive,
			imageVariants=imageVariants,
		)
		return self.fetch(url, sid, preferredLanguage, timeout, force)

	def fetch(self, url, sid, preferredLanguage, timeout, force):
		fetched = self.fetcher.fetch(url, timeout=timeout, force=force)
		try:
			events = self.parser.parse(
				fetched.content,
				preferredLanguage=preferredLanguage,
				contentGuideServiceRef=sid,
			)
			self.fetcher.markGood(url, fetched.content)
		except Exception:
			lastGood = self.fetcher.readLastGood(url)
			if not lastGood or lastGood == fetched.content:
				raise
			events = self.parser.parse(
				lastGood,
				preferredLanguage=preferredLanguage,
				contentGuideServiceRef=sid,
			)
			fetched.stale = True
		return ContentGuideResponse(url, sid, events, fetched)


def guideSlug(value):
	value = value or "dvbi"
	value = value.lower()
	value = reSub(r"[^a-z0-9]+", ".", value)
	value = reSub(r"\.+", ".", value).strip(".")
	return value or "dvbi"


def xmlEscape(value):
	return escape(str(value or ""), {'"': "&quot;"})


class EpgBridge:
	"""Prepare DVB-I ContentGuide metadata for a later EPG importer phase."""

	def __init__(self, dataDir):
		self.dataDir = dataDir
		if not pathExists(self.dataDir):
			makedirs(self.dataDir)

	def writeSources(self, serviceList, exportXmltv=False, xmltvPath="", exportProbe=True):
		"""Write ContentGuide source metadata and stable service mapping."""

		def resolveSource(service, sources):
			explicit = getattr(service, "contentGuideSourceRef", "")
			refs = [explicit] if explicit else []

			for ref in refs:
				if ref in sources:
					return ref, sources.get(ref, {}), "explicit_ref"

			if refs:
				return refs[0], {}, "unresolved_ref"

			if len(sources) == 1:
				sourceId = sorted(sources.keys())[0]
				return sourceId, sources.get(sourceId, {}), "single_source_fallback"

			return "", {}, "none"

		def getChannelId(service):
			seed = service.dvbiId or "|".join([service.country or "", service.provider or "", service.name or ""])
			digest = sha1(seed.encode("utf-8")).hexdigest()[:20]
			return "dvbi.{0}.{1}".format(guideSlug(service.country or "global"), digest)

		listIdentity = getattr(serviceList, "listId", "") or serviceList.sourceUrl
		listKey = sha1(listIdentity.encode("utf-8")).hexdigest()[:16]
		outputDir = join(self.dataDir, "epg", listKey)
		if not pathExists(outputDir):
			makedirs(outputDir)
		contentGuidePath = join(outputDir, "content_guide_sources.json")
		serviceMapPath = join(outputDir, "epg_service_map.json")

		sources = serviceList.contentGuideSources or {}
		serviceMap = []
		servicesWithContentGuide = 0
		servicesWithSchedule = 0

		for service in serviceList.services:
			service.epgChannelId = getChannelId(service)
			sourceId, source, resolution = resolveSource(service, sources)
			hasContentGuide = bool(sourceId)
			hasSchedule = bool(source.get("schedule", "")) if source else False
			if hasContentGuide:
				servicesWithContentGuide += 1
			if hasSchedule:
				servicesWithSchedule += 1

			serviceMap.append(
				{
					"epg_channel_id": service.epgChannelId,
					"dvbi_id": service.dvbiId,
					"name": service.name,
					"provider": service.provider,
					"country": service.country,
					"language": service.language,
					"regions": service.regions,
					"lcn": service.lcn,
					"enigma2_ref": service.selectedRef or service.matchedRef,
					"selected_instance_type": service.selectedInstanceType,
					"service_kind": getattr(service, "mediaKind", "tv"),
					"service_kind_source": getattr(service, "mediaKindSource", "default_tv"),
					"service_type_uri": getattr(service, "serviceTypeUri", ""),
					"service_type_term": getattr(service, "serviceTypeTerm", ""),
					"service_type": getattr(service, "serviceTypeTerm", ""),
					"status": service.status,
					"logo_url": service.logoUrls[0] if service.logoUrls else "",
					"content_guide_source_ref": getattr(service, "contentGuideSourceRef", ""),
					"content_guide_service_ref": getattr(service, "contentGuideServiceRef", "") or service.dvbiId,
					"content_guide_resolution": resolution,
					"content_guide_source": source,
					"has_content_guide": hasContentGuide,
					"has_schedule_endpoint": hasSchedule,
				}
			)

		contentData = {
			"updated_at": int(time()),
			"source_url": serviceList.sourceUrl,
			"service_list": serviceList.name,
			"country": serviceList.country,
			"language": serviceList.language,
			"content_guide_sources": sources,
			"stats": {
				"content_guide_sources_total": len(sources),
				"services_total": len(serviceList.services),
				"services_with_content_guide": servicesWithContentGuide,
				"services_with_schedule_endpoint": servicesWithSchedule,
			},
		}

		mapData = {
			"updated_at": int(time()),
			"source_url": serviceList.sourceUrl,
			"service_list": serviceList.name,
			"country": serviceList.country,
			"language": serviceList.language,
			"services": serviceMap,
			"stats": contentData["stats"],
		}

		atomicWriteJson(contentGuidePath, contentData)
		atomicWriteJson(serviceMapPath, mapData)

		exportedXmltvPath = ""
		if exportXmltv:
			exportedXmltvPath = self.writeXmltvChannelExport(serviceList, serviceMap, xmltvPath or join(outputDir, "dvbi_channels.xml"))

		probePath = ""
		probeCount = 0
		if exportProbe:
			probeResult = self.writeEndpointProbeExport(serviceList, serviceMap, outputDir=outputDir)
			probePath = probeResult.get("path", "")
			probeCount = probeResult.get("count", 0)

		return {
			"content_guide_path": contentGuidePath,
			"service_map_path": serviceMapPath,
			"xmltv_path": exportedXmltvPath,
			"epg_probe_path": probePath,
			"stats": {
				"content_guide_sources_total": len(sources),
				"services_total": len(serviceList.services),
				"services_with_content_guide": servicesWithContentGuide,
				"services_with_schedule_endpoint": servicesWithSchedule,
				"xmltv_channels": len(serviceMap) if exportXmltv else 0,
				"epg_probe_services": probeCount,
			},
		}

	def writeEndpointProbeExport(self, serviceList, serviceMap, outputDir=""):
		"""Write debug URLs for ScheduleInfoEndpoint exploration.

		This intentionally does not fetch EPG data. Many ContentGuide endpoints
		do not return useful data when opened directly in a browser; they expect
		service and time-window query parameters. The generated JSON/TXT files
		are only diagnostics for the next EPG phase.
		"""
		outputDir = outputDir or self.dataDir
		jsonPath = join(outputDir, "epg_endpoint_probe.json")
		textPath = join(outputDir, "epg_endpoint_probe.txt")
		# TS 103 770 timestamp windows start at a 10,800-second boundary and
		# are exactly six or twelve hours long.
		startUnix = (int(time()) // 10800) * 10800
		endUnix = startUnix + (6 * 60 * 60)

		entries = []
		for item in serviceMap:
			source = item.get("content_guide_source") or {}
			endpoint = source.get("schedule") or ""
			if not endpoint:
				continue

			serviceId = item.get("content_guide_service_ref") or item.get("dvbi_id") or ""
			epgChannelId = item.get("epg_channel_id") or ""
			candidates = [
				{
					"name": "raw_endpoint",
					"url": endpoint,
					"note": "Often not useful in a browser because schedule endpoints usually need service and time-window parameters.",
				},
				{
					"name": "timestamp_6h",
					"url": self.urlWithQuery(endpoint, {"sid": serviceId, "start": startUnix, "end": endUnix}),
				},
				{
					"name": "now_next",
					"url": self.urlWithQuery(endpoint, {"sid": serviceId, "now_next": "true"}),
				},
			]
			entries.append(
				{
					"name": item.get("name", ""),
					"provider": item.get("provider", ""),
					"dvbi_id": item.get("dvbi_id", ""),
					"epg_channel_id": epgChannelId,
					"schedule_endpoint": endpoint,
					"candidates": candidates,
				}
			)

		data = {
			"updated_at": int(time()),
			"source_url": serviceList.sourceUrl,
			"service_list": serviceList.name,
			"note": "Diagnostic only. Query shapes follow ETSI TS 103 770 ScheduleInfoEndpoint rules.",
			"time_window": {
				"start_unix": startUnix,
				"end_unix": endUnix,
				"duration_seconds": endUnix - startUnix,
			},
			"services": entries,
		}

		atomicWriteJson(jsonPath, data)

		lines = []
		lines.append("DVB-I ContentGuide endpoint probe URLs\n")
		lines.append("======================================\n")
		lines.append("These URLs are diagnostics only. Raw ScheduleInfoEndpoint URLs often show nothing in a browser.\n")
		lines.append("Start Unix: {0}\nEnd Unix: {1}\n\n".format(startUnix, endUnix))
		for entry in entries:
			lines.append("{0} ({1})\n".format(entry.get("name", ""), entry.get("provider", "")))
			lines.append("  DVB-I ID: {0}\n".format(entry.get("dvbi_id", "")))
			lines.append("  Endpoint: {0}\n".format(entry.get("schedule_endpoint", "")))
			for candidate in entry.get("candidates", []):
				lines.append("  - {0}: {1}\n".format(candidate.get("name", ""), candidate.get("url", "")))
			lines.append("\n")
		atomicWrite(textPath, "".join(lines))

		return {
			"path": jsonPath,
			"text_path": textPath,
			"count": len(entries),
		}

	def urlWithQuery(self, url, params):
		parsed = urlparse(url)
		existing = parseQsl(parsed.query, keep_blank_values=True)
		merged = list(existing)
		for key, value in params.items():
			if value is None or value == "":
				continue
			merged.append((key, str(value)))
		return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(merged), parsed.fragment))

	def writeXmltvChannelExport(self, serviceList, serviceMap, xmltvPath=""):
		"""Write a channel-only XMLTV file for mapping tests."""
		if not xmltvPath:
			xmltvPath = join(self.dataDir, "dvbi_channels.xml")
		directory = dirname(xmltvPath)
		if directory and not pathExists(directory):
			makedirs(directory)

		lang = serviceList.language or ""
		lines = []
		lines.append('<?xml version="1.0" encoding="UTF-8"?>\n')
		lines.append('<tv generator-info-name="OpenATV DVB-I Manager">\n')
		lines.append("  <!-- Channel-only XMLTV test export. No programme data is fetched in phase 1.7. -->\n")
		for item in serviceMap:
			channelId = xmlEscape(item.get("epg_channel_id", ""))
			name = xmlEscape(item.get("name", ""))
			provider = xmlEscape(item.get("provider", ""))
			logo = item.get("logo_url", "")
			lines.append('  <channel id="{0}">\n'.format(channelId))
			if lang:
				lines.append('    <display-name lang="{0}">{1}</display-name>\n'.format(xmlEscape(lang), name))
			else:
				lines.append("    <display-name>{0}</display-name>\n".format(name))
			if provider:
				lines.append("    <display-name>{0} - {1}</display-name>\n".format(provider, name))
			if logo:
				lines.append('    <icon src="{0}" />\n'.format(xmlEscape(logo)))
			lines.append("  </channel>\n")
		lines.append("</tv>\n")

		atomicWrite(xmltvPath, "".join(lines))

		return xmltvPath


MAX_SERVICES_PER_SYNC = 500


class EpgSynchronizer:
	def __init__(self, cacheDir, maxWorkers=4, timeout=15):
		self.cacheDir = cacheDir
		self.maxWorkers = max(1, min(int(maxWorkers), 8))
		self.timeout = max(3, min(int(timeout), 60))

	def syncNowNext(self, serviceList, force=False):
		def fetchOne(service, endpoint, sid, force):
			client = ContentGuideClient(cacheDir=self.cacheDir)
			response = client.fetchNowNext(
				endpoint,
				sid,
				preferredLanguage=service.language,
				timeout=self.timeout,
				force=force,
			)
			return {
				"dvbi_id": service.dvbiId,
				"name": service.name,
				"service_reference": service.selectedRef,
				"content_guide_service_ref": sid,
				"import_events": response.e2Events,
				"event_count": len(response.events),
				"http_status": response.fetchResult.status,
				"http_stale": bool(getattr(response.fetchResult, "stale", False)),
			}

		def sourceForService(serviceList, service):
			sourceRef = getattr(service, "contentGuideSourceRef", "")
			if sourceRef:
				return serviceList.contentGuideSources.get(sourceRef, {})
			if len(serviceList.contentGuideSources) == 1:
				return next(iter(serviceList.contentGuideSources.values()))
			return {}

		tasks = []
		for service in serviceList.services:
			if not service.selectedRef:
				continue
			source = sourceForService(serviceList, service)
			endpoint = source.get("schedule", "") if source else ""
			sid = getattr(service, "contentGuideServiceRef", "") or service.dvbiId
			if endpoint and sid:
				tasks.append((service, endpoint, sid))
		eligibleServices = len(tasks)
		servicesTruncated = max(0, eligibleServices - MAX_SERVICES_PER_SYNC)
		if servicesTruncated:
			tasks = tasks[:MAX_SERVICES_PER_SYNC]

		entries = []
		errors = []
		with ThreadPoolExecutor(max_workers=self.maxWorkers) as executor:
			futures = {executor.submit(fetchOne, service, endpoint, sid, force): (service, endpoint, sid) for service, endpoint, sid in tasks}
			for future in asCompleted(futures):
				service, endpoint, sid = futures[future]
				try:
					entries.append(future.result())
				except Exception as error:
					errors.append(
						{
							"dvbi_id": service.dvbiId,
							"name": service.name,
							"service_reference": service.selectedRef,
							"content_guide_service_ref": sid,
							"endpoint": endpoint,
							"error": str(error),
						}
					)

		entries.sort(key=lambda item: (item.get("service_reference", ""), item.get("content_guide_service_ref", "")))
		errors.sort(key=lambda item: (item.get("name", ""), item.get("content_guide_service_ref", "")))
		return {
			"enabled": True,
			"services_considered": len(tasks),
			"services_eligible": eligibleServices,
			"services_truncated": servicesTruncated,
			"services_synchronised": len(entries),
			"services_failed": len(errors),
			"events_total": sum(item.get("event_count", 0) for item in entries),
			"entries": entries,
			"errors": errors,
		}


# Validated logos for the existing E2 picon paths


class LogoCache:
	"""Download validated service logos into a separate DVB-I cache.

	Cached files are always PNG. Native PNG data can therefore be handled
	without an image library. JPEG and WebP are accepted only when Pillow is
	available and can safely rasterise them to PNG. SVG is deliberately not
	accepted: XML-based images need a dedicated, hardened rasteriser which is
	not guaranteed to be installed on an OpenATV receiver.
	"""

	USER_AGENT = "OpenATV-DvbIManager/0.3.6"
	DEFAULT_TIMEOUT = 10
	DEFAULT_MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024
	DEFAULT_MAX_DIMENSION = 4096
	DEFAULT_MAX_PIXELS = 16 * 1024 * 1024
	MAX_URL_LENGTH = 4096
	READ_CHUNK_SIZE = 64 * 1024

	MIME_FORMATS = {
		"image/png": "png",
		"image/jpeg": "jpeg",
		"image/webp": "webp",
	}
	SAFE_PICON_NAME = reCompile(r"^[A-Za-z0-9_.%+\-]+$")

	def __init__(self, cacheDir, installPicons=True, maxDownloadBytes=DEFAULT_MAX_DOWNLOAD_BYTES, timeout=DEFAULT_TIMEOUT, maxDimension=DEFAULT_MAX_DIMENSION, maxPixels=DEFAULT_MAX_PIXELS, piconDirs=()):
		def loadMapping():
			if not pathExists(self.mappingPath):
				return {}
			try:
				with open(self.mappingPath, "r", encoding="utf-8") as handle:
					mapping = jsonLoad(handle)
				return mapping if isinstance(mapping, dict) else {}
			except Exception:
				return {}

		self.cacheDir = cacheDir
		self.piconDirs = list(piconDirs)
		self.installPicons = bool(installPicons)
		self.maxDownloadBytes = self.positiveInt(maxDownloadBytes, "max_download_bytes")
		self.timeout = self.positiveNumber(timeout, "timeout")
		self.maxDimension = self.positiveInt(maxDimension, "max_dimension")
		self.maxPixels = self.positiveInt(maxPixels, "max_pixels")
		self.ensureDirectory(self.cacheDir)
		self.mappingPath = join(self.cacheDir, "logo_mapping.json")
		self.mapping = loadMapping()

	@staticmethod
	def positiveInt(value, name):
		try:
			value = int(value)
		except (TypeError, ValueError) as error:
			raise ValueError(_("{0} must be a positive integer").format(name)) from error
		if value <= 0:
			raise ValueError(_("{0} must be a positive integer").format(name))
		return value

	@staticmethod
	def ensureDirectory(path):
		try:
			makedirs(path)
		except OSError:
			# Concurrent workers may both observe a missing directory. Only
			# suppress the error when the desired directory now exists.
			if not isdir(path):
				raise

	@staticmethod
	def positiveNumber(value, name):
		try:
			value = float(value)
		except (TypeError, ValueError) as error:
			raise ValueError(_("{0} must be positive").format(name)) from error
		if value <= 0:
			raise ValueError(_("{0} must be positive").format(name))
		return value

	def saveMapping(self):
		"""Publish mapping JSON atomically so interrupted writes stay usable."""
		directory = dirname(self.mappingPath) or "."
		prefix = ".{0}.".format(basename(self.mappingPath))
		descriptor, temporaryPath = mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
		try:
			with fdopen(descriptor, "w", encoding="utf-8") as handle:
				descriptor = None
				jsonDump(self.mapping, handle, indent=2, sort_keys=True)
				handle.write("\n")
				handle.flush()
				fsync(handle.fileno())
			replace(temporaryPath, self.mappingPath)
			temporaryPath = ""
		finally:
			if descriptor is not None:
				close(descriptor)
			if temporaryPath:
				try:
					unlink(temporaryPath)
				except OSError:
					pass

	def cacheServiceLogos(self, services, maxCount=0):
		"""Cache the first usable logo per service.

		A service list can advertise several logo variants. Invalid, oversized
		or unsupported variants (notably SVG) are skipped so that a later PNG
		candidate can still be used.
		"""
		cached = 0
		for service in services:
			if maxCount and cached >= maxCount:
				break
			if not service.logoUrls:
				continue

			path = ""
			for url in service.logoUrls:
				try:
					path = self.cacheLogo(service.dvbiId or service.name, url)
				except Exception:
					path = ""
				if path:
					break

			if path:
				cached += 1
				self.installServicePicons(service, path)

		self.saveMapping()
		return cached

	def cacheLogo(self, serviceId, url, timeout=None):
		"""Download and validate one logo, returning its PNG cache path.

		The response body is read with a hard upper bound. MIME type and file
		signature must agree. A cache entry is published only after the full
		PNG has passed validation.
		"""

		def detectFormat(content):
			if content.startswith(b"\x89PNG\r\n\x1a\n"):
				return "png"
			if content.startswith(b"\xff\xd8\xff"):
				return "jpeg"
			if len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP":
				return "webp"
			return ""

		def readBounded(response):
			chunks = []
			total = 0
			while True:
				remaining = self.maxDownloadBytes + 1 - total
				if remaining <= 0:
					return b""
				chunk = response.read(min(self.READ_CHUNK_SIZE, remaining))
				if not chunk:
					break
				if not isinstance(chunk, bytes):
					return b""
				chunks.append(chunk)
				total += len(chunk)
				if total > self.maxDownloadBytes:
					return b""
			return b"".join(chunks)

		serviceId = str(serviceId or "")
		if not serviceId:
			return ""
		url = self.validateUrl(url)
		effectiveTimeout = self.timeout if timeout is None else self.positiveNumber(timeout, "timeout")

		serviceKey = sha1(serviceId.encode("utf-8")).hexdigest()
		target = join(self.cacheDir, serviceKey + ".png")
		previous = self.mapping.get(serviceId)
		if isinstance(previous, dict) and previous.get("url") == url and abspath(previous.get("path", "")) == abspath(target) and self.validCachedPng(target):
			return target

		request = Request(url)
		request.add_header("User-Agent", self.USER_AGENT)
		request.add_header("Accept", "image/png, image/jpeg, image/webp")
		response = urlopen(request, timeout=effectiveTimeout)
		try:
			status = getattr(response, "status", None)
			if status is None and hasattr(response, "getcode"):
				status = response.getcode()
			if status is not None and not 200 <= int(status) < 300:
				return ""

			finalUrl = response.geturl() if hasattr(response, "geturl") else url
			finalUrl = self.validateUrl(finalUrl)
			if urlsplit(url).scheme.lower() == "https" and urlsplit(finalUrl).scheme.lower() != "https":
				return ""

			contentType = self.responseHeader(response, "Content-Type")
			mimeType = (contentType or "").split(";", 1)[0].strip().lower()
			expectedFormat = self.MIME_FORMATS.get(mimeType)
			if expectedFormat is None:
				return ""

			contentLength = self.responseHeader(response, "Content-Length")
			if contentLength not in (None, ""):
				try:
					contentLength = int(contentLength)
				except (TypeError, ValueError):
					return ""
				if contentLength < 0 or contentLength > self.maxDownloadBytes:
					return ""

			content = readBounded(response)
			if not content:
				return ""
		finally:
			try:
				response.close()
			except Exception:
				pass

		actualFormat = detectFormat(content)
		if actualFormat != expectedFormat:
			return ""

		if actualFormat == "png":
			pngContent = content if self.validatePng(content) else b""
		else:
			pngContent = self.convertWithPillow(content, actualFormat)
			if pngContent and not self.validatePng(pngContent):
				pngContent = b""
		if not pngContent or len(pngContent) > self.maxDownloadBytes:
			return ""

		self.atomicWrite(target, pngContent)
		self.mapping[serviceId] = {
			"url": url,
			"path": target,
		}
		return target

	def validateUrl(self, url):
		if not isinstance(url, str):
			raise ValueError(_("logo URL must be text"))
		if not url or url != url.strip() or len(url) > self.MAX_URL_LENGTH:
			raise ValueError(_("invalid logo URL"))
		if any(ord(character) < 32 or ord(character) == 127 for character in url):
			raise ValueError(_("invalid logo URL"))

		parsed = urlsplit(url)
		if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc or not parsed.hostname:
			raise ValueError(_("logo URL must use HTTP or HTTPS"))
		if parsed.username is not None or parsed.password is not None:
			raise ValueError(_("credentials are not allowed in logo URLs"))
		try:
			parsed.port
		except ValueError as error:
			raise ValueError(_("invalid logo URL port")) from error

		hostname = parsed.hostname.rstrip(".").lower()
		if hostname == "localhost" or hostname.endswith(".localhost") or hostname.endswith(".local"):
			raise ValueError(_("local logo hosts are not allowed"))
		try:
			address = ipAddress(hostname)
		except ValueError:
			address = None
		if address is not None and not address.is_global:
			raise ValueError(_("non-public logo addresses are not allowed"))
		return url

	@staticmethod
	def responseHeader(response, name):
		headers = getattr(response, "headers", None)
		if headers is not None and hasattr(headers, "get"):
			return headers.get(name)
		if hasattr(response, "info"):
			info = response.info()
			if info is not None and hasattr(info, "get"):
				return info.get(name)
		return None

	def validatePng(self, content):
		"""Validate PNG structure, CRCs and decoder-relevant dimensions."""
		if len(content) < 45 or len(content) > self.maxDownloadBytes:
			return False
		if not content.startswith(b"\x89PNG\r\n\x1a\n"):
			return False

		offset = 8
		firstChunk = True
		foundIdat = False
		foundIend = False
		while offset < len(content):
			if offset + 12 > len(content):
				return False
			length = unpack(">I", content[offset: offset + 4])[0]
			chunkType = content[offset + 4: offset + 8]
			chunkEnd = offset + 12 + length
			if chunkEnd > len(content):
				return False
			chunkData = content[offset + 8: offset + 8 + length]
			storedCrc = unpack(">I", content[offset + 8 + length: chunkEnd])[0]
			if crc32(chunkType + chunkData) & 0xFFFFFFFF != storedCrc:
				return False

			if firstChunk:
				if chunkType != b"IHDR" or length != 13:
					return False
				width, height = unpack(">II", chunkData[:8])
				if width <= 0 or height <= 0 or width > self.maxDimension or height > self.maxDimension or width * height > self.maxPixels:
					return False
				bitDepth, colourType, compression, filtering, interlace = unpack(">BBBBB", chunkData[8:13])
				validDepths = {
					0: (1, 2, 4, 8, 16),
					2: (8, 16),
					3: (1, 2, 4, 8),
					4: (8, 16),
					6: (8, 16),
				}
				if colourType not in validDepths or bitDepth not in validDepths[colourType]:
					return False
				if compression != 0 or filtering != 0 or interlace not in (0, 1):
					return False
			elif chunkType == b"IHDR":
				return False

			if chunkType == b"IDAT":
				foundIdat = True
			elif chunkType == b"IEND":
				if length != 0 or chunkEnd != len(content):
					return False
				foundIend = True
				offset = chunkEnd
				break

			firstChunk = False
			offset = chunkEnd

		return foundIdat and foundIend and offset == len(content)

	def convertWithPillow(self, content, sourceFormat):
		"""Return PNG bytes, or empty bytes when Pillow is unavailable/unsafe."""
		try:
			from PIL import Image
		except (ImportError, ModuleNotFoundError):
			return b""

		try:
			with catchWarnings():
				decompressionWarning = getattr(Image, "DecompressionBombWarning", RuntimeWarning)
				simplefilter("error", decompressionWarning)
				with Image.open(BytesIO(content)) as image:
					if (image.format or "").lower() != sourceFormat:
						return b""
					width, height = image.size
					if width <= 0 or height <= 0 or width > self.maxDimension or height > self.maxDimension or width * height > self.maxPixels:
						return b""
					image.seek(0)
					image.load()
					if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
						image = image.convert("RGBA")
					else:
						image = image.convert("RGB")
					output = BytesIO()
					image.save(output, format="PNG")
					return output.getvalue()
		except Exception:
			return b""

	def validCachedPng(self, path):
		try:
			if not isfile(path) or getsize(path) > self.maxDownloadBytes:
				return False
			with open(path, "rb") as handle:
				content = handle.read(self.maxDownloadBytes + 1)
			return self.validatePng(content)
		except OSError:
			return False

	@staticmethod
	def atomicWrite(target, content):
		directory = dirname(target) or "."
		prefix = ".{0}.".format(basename(target))
		descriptor, temporaryPath = mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
		try:
			with fdopen(descriptor, "wb") as handle:
				descriptor = None
				handle.write(content)
				handle.flush()
				fsync(handle.fileno())
			replace(temporaryPath, target)
			temporaryPath = ""
		finally:
			if descriptor is not None:
				close(descriptor)
			if temporaryPath:
				try:
					unlink(temporaryPath)
				except OSError:
					pass

	def installServicePicons(self, service, cachedLogoPath):
		"""Expose cached DVB-I logos as non-destructive normal PNG picons."""
		if not self.installPicons or not self.piconDirs or not cachedLogoPath:
			return
		if not service.selectedRef or not self.validCachedPng(cachedLogoPath):
			return

		names = self.piconNamesForRef(service.selectedRef)
		# The logical broadcast reference remains visible during hybrid playback.
		# Include every matched tuner variant, not only the selected bouquet ref.
		for reference in list(getattr(service, "matchedRefs", [])) + list(getattr(service, "hybridRefs", [])):
			for name in self.piconNamesForRef(reference):
				if name not in names:
					names.append(name)
		if not names:
			return

		source = abspath(cachedLogoPath)
		for directory in self.piconDirs:
			for name in names:
				target = join(directory, name + ".png")
				if lexists(target):
					continue
				try:
					# Creating the final symlink is atomic and fails on a race.
					symlink(source, target)
				except OSError:
					self.atomicCopyNoReplace(source, target)

	@staticmethod
	def atomicCopyNoReplace(source, target):
		"""Copy without replacing a picon; publish atomically where supported."""
		directory = dirname(target) or "."
		prefix = ".{0}.".format(basename(target))
		descriptor, temporaryPath = mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
		try:
			with fdopen(descriptor, "wb") as destination:
				descriptor = None
				with open(source, "rb") as sourceHandle:
					while True:
						chunk = sourceHandle.read(64 * 1024)
						if not chunk:
							break
						destination.write(chunk)
				destination.flush()
				fsync(destination.fileno())

			try:
				# Hard-linking a complete temporary file gives us an atomic,
				# no-replace publish operation even if another worker wins.
				link(temporaryPath, target)
			except FileExistsError:
				pass
			except OSError as error:
				if error.errno in (EPERM, EOPNOTSUPP, ENOSYS, EXDEV):
					# FAT/exFAT cannot link files. Exclusive creation still
					# protects user picons, but readers can see the brief copy.
					LogoCache.copyExclusive(temporaryPath, target)
		except OSError:
			# Picon export is optional and must not invalidate a successfully
			# cached logo when the target storage disappears or becomes full.
			pass
		finally:
			if descriptor is not None:
				close(descriptor)
			try:
				unlink(temporaryPath)
			except OSError:
				pass

	@staticmethod
	def copyExclusive(source, target):
		ownedFile = None
		try:
			with open(source, "rb") as origin, open(target, "xb") as destination:
				ownedFile = fstat(destination.fileno())
				while True:
					chunk = origin.read(64 * 1024)
					if not chunk:
						break
					destination.write(chunk)
				destination.flush()
				fsync(destination.fileno())
		except OSError:
			# Remove a partial copy only if the path still names our file.
			if ownedFile is not None:
				try:
					if samestat(ownedFile, fileStat(target, follow_symlinks=False)):
						unlink(target)
				except OSError:
					pass

	def piconNamesForRef(self, serviceRef):
		ref = (serviceRef or "").strip()
		if not ref:
			return []
		parts = ref.split(":")
		names = []

		# Primary Enigma2 picon key: first 10 service reference fields.
		if len(parts) >= 10:
			firstTen = "_".join(parts[:10]).rstrip("_")
			if self.isSafePiconName(firstTen):
				names.append(firstTen)

		# Fallback key is retained only when it is a single safe path element.
		normalized = ref.replace(":", "_").rstrip("_")
		if normalized not in names and self.isSafePiconName(normalized):
			names.append(normalized)

		return names

	def isSafePiconName(self, name):
		return bool(name and len(name) < 220 and name not in (".", "..") and self.SAFE_PICON_NAME.match(name))


# Import and registry tasks


class DvbIManager:
	"""Main DVB-I import manager."""

	DEFAULT_DATA_DIR = "/media/hdd/dvbi"
	FALLBACK_DATA_DIR = "/etc/enigma2/dvbi"
	FALLBACK_RUNTIME_DIR = "/tmp/dvbi"

	def __init__(self, dataDir=None, enigma2Dir="/etc/enigma2", logger=None):
		self.logger = logger or self.defaultLogger
		self.stalePlaybackTokenPaths = []
		requestedDataDir = dataDir or self.DEFAULT_DATA_DIR
		self.dataDir = self.resolveDataDir(requestedDataDir)
		self.usingFlashFallback = normcase(normpath(self.dataDir)) == normcase(normpath(self.FALLBACK_DATA_DIR))
		self.runtimeDir = self.FALLBACK_RUNTIME_DIR if self.usingFlashFallback else self.dataDir
		self.enigma2Dir = enigma2Dir
		self.cacheDir = join(self.runtimeDir, "cache")
		self.httpCacheDir = join(self.cacheDir, "http")
		self.logoCacheDir = join(self.runtimeDir, "picons")
		self.metadataDir = join(self.dataDir, "metadata")
		self.logsDir = join(self.runtimeDir, "logs")

		for path in [self.dataDir, self.runtimeDir, self.cacheDir, self.httpCacheDir, self.logoCacheDir, self.metadataDir, self.logsDir]:
			if not pathExists(path):
				makedirs(path)

	def resolveDataDir(self, requestedDir):
		"""Return a safe data directory. Avoid filling rootfs when /media/hdd is not mounted."""
		requestedDir = requestedDir or self.DEFAULT_DATA_DIR
		requestedDir = normpath(requestedDir)

		if self.needsExternalMount(requestedDir) and not self.isOnExternalMount(requestedDir):
			self.defaultLogger("Data medium is not mounted; using fallback data directory {0}".format(self.FALLBACK_DATA_DIR))
			return self.FALLBACK_DATA_DIR

		try:
			if not pathExists(requestedDir):
				makedirs(requestedDir)
			testPath = join(requestedDir, ".write_test")
			with open(testPath, "w") as handle:
				handle.write("ok")
			try:
				remove(testPath)
			except Exception:
				pass
			return requestedDir
		except Exception:
			self.defaultLogger("Data directory {0} is not writable; using fallback {1}".format(requestedDir, self.FALLBACK_DATA_DIR))
			if not pathExists(self.FALLBACK_DATA_DIR):
				makedirs(self.FALLBACK_DATA_DIR)
			return self.FALLBACK_DATA_DIR

	def needsExternalMount(self, path):
		normalized = normpath(path)
		return normalized in ("/media", "/mnt", "/run/media") or normalized.startswith(("/media/", "/mnt/", "/run/media/"))

	def isOnExternalMount(self, path):
		"""Reject mount containers (notably OpenATV's tiny /media tmpfs)."""
		normalized = realpath(normpath(path))
		try:
			with open("/proc/mounts", "r") as handle:
				mountpoints = []
				for line in handle:
					parts = line.split()
					if len(parts) > 2:
						mounted = realpath(normpath(parts[1].replace("\\040", " ")))
						mountpoints.append((mounted, parts[2]))
			for mounted, filesystem in sorted(mountpoints, key=lambda item: len(item[0]), reverse=True):
				if normalized == mounted or normalized.startswith(mounted.rstrip("/") + "/"):
					return mounted not in ("/", "/media", "/mnt", "/run/media") and filesystem not in ("tmpfs", "ramfs", "autofs", "devtmpfs", "proc", "sysfs")
		except Exception:
			pass
		# Without mount information it is unsafe to assume persistent storage.
		return False

	def defaultLogger(self, message):
		try:
			print("[DvbIManager] {0}".format(message))
		except Exception:
			pass

	def importUrl(self, url, options=None):
		"""Import one DVB-I service list URL."""

		def writeMetadata(serviceList, fetchResult, bouquetResult, matched, logoCount, epgResult, ipServiceType, piconDirs, installPicons, reportPath, report, preferBroadcast, exportXmltv):
			data = {
				"updated_at": int(time()),
				"service_list": {
					"id": getattr(serviceList, "listId", ""),
					"version": getattr(serviceList, "version", ""),
					"response_status": getattr(serviceList, "responseStatus", ""),
					"schema_namespace": getattr(serviceList, "schemaNamespace", ""),
					"name": serviceList.name,
					"provider": serviceList.provider,
					"source_url": serviceList.sourceUrl,
					"country": serviceList.country,
					"language": serviceList.language,
					"region": serviceList.region,
					"region_catalog": getattr(serviceList, "regionCatalog", []),
					"region_catalog_version": getattr(serviceList, "regionCatalogVersion", None),
					"region_catalog_language": getattr(serviceList, "regionCatalogLanguage", ""),
				},
				"fetch": {
					"changed": fetchResult.changed,
					"status": fetchResult.status,
					"stale": bool(getattr(fetchResult, "stale", False)),
					"cache_file": fetchResult.cacheFile,
				},
				"bouquet": bouquetResult,
				"stats": {
					"services_total": len(serviceList.services),
					"services_matched_broadcast": matched,
					"services_written": bouquetResult.get("added", 0),
					"services_written_tv": bouquetResult.get("services_written_tv", 0),
					"services_written_radio": bouquetResult.get("services_written_radio", 0),
					"services_written_broadcast": bouquetResult.get("broadcast_written", 0),
					"services_written_ip": bouquetResult.get("ip_written", 0),
					"services_skipped": bouquetResult.get("skipped", 0),
					"services_drm_marked": bouquetResult.get("drm_marked", 0),
					"services_hbbtv_marked": bouquetResult.get("hbbtv_marked", 0),
					"logos_cached": logoCount,
				},
				"report": report,
				"report_path": reportPath,
				"epg_metadata_path": epgResult.get("content_guide_path", ""),
				"epg_service_map_path": epgResult.get("service_map_path", ""),
				"epg_xmltv_path": epgResult.get("xmltv_path", ""),
				"epg_probe_path": epgResult.get("epg_probe_path", ""),
				"epg": epgResult,
				"export_xmltv": bool(exportXmltv),
				"ip_service_type": ipServiceType,
				"prefer_broadcast": bool(preferBroadcast),
				"picon_dirs": piconDirs,
				"install_picons": installPicons,
				"data_dir": self.dataDir,
				"cache_dir": self.cacheDir,
				"logo_cache_dir": self.logoCacheDir,
				"logs_dir": self.logsDir,
				"services": [],
			}

			for service in serviceList.services:
				data["services"].append(
					{
						"dvbi_id": service.dvbiId,
						"name": service.name,
						"provider": service.provider,
						"country": service.country,
						"language": service.language,
						"regions": service.regions,
						"lcn": service.lcn,
						"flags": service.flags,
						"status": service.status,
						"matched_ref": service.matchedRef,
						"selected_ref": service.selectedRef,
						"selected_instance_type": service.selectedInstanceType,
						"selected_player_type": getattr(service, "selectedPlayerType", ""),
						"selected_media_kind": getattr(service, "selectedMediaKind", ""),
						"service_type_uri": getattr(service, "serviceTypeUri", ""),
						"service_type_term": getattr(service, "serviceTypeTerm", ""),
						"service_media_kind": getattr(service, "mediaKind", "tv"),
						"service_media_kind_source": getattr(service, "mediaKindSource", "default_tv"),
						"service_kind": getattr(service, "mediaKind", "tv"),
						"service_kind_source": getattr(service, "mediaKindSource", "default_tv"),
						"logo_urls": service.logoUrls,
						"content_guide_source_ref": getattr(service, "contentGuideSourceRef", ""),
						"content_guide_service_ref": getattr(service, "contentGuideServiceRef", ""),
						"epg_channel_id": getattr(service, "epgChannelId", ""),
						"instances": [
							{
								"type": instance.instanceType,
								"url": instance.url,
								"sid": instance.serviceId,
								"tsid": instance.transportStreamId,
								"onid": instance.originalNetworkId,
								"namespace": instance.namespace,
								"priority": getattr(instance, "priority", None),
								"content_type": getattr(instance, "contentType", ""),
								"detected_content_type": getattr(instance, "detectedContentType", ""),
								"detected_media_kind": getattr(instance, "detectedMediaKind", ""),
								"media_probe_status": getattr(instance, "mediaProbeStatus", ""),
								"media_probe_evidence": getattr(instance, "mediaProbeEvidence", ""),
								"drm": instance.drm,
								"fta_verified": getattr(instance, "ftaVerified", False),
								"manifest_probe_status": getattr(instance, "manifestProbeStatus", ""),
								"playback_unavailable": getattr(instance, "playbackUnavailable", False),
								"availability": getattr(instance, "raw", {}).get("manifest_probe", {}).get("availability", {"status": "unknown"}),
								"drm_system_ids": getattr(instance, "drmSystemIds", []),
								"hbbtv": instance.hbbtv,
							}
							for instance in service.instances
						],
					}
				)

			listIdentity = getattr(serviceList, "listId", "") or serviceList.sourceUrl
			listKey = sha1(listIdentity.encode("utf-8")).hexdigest()[:16]
			listsDir = join(self.metadataDir, "lists")
			if not pathExists(listsDir):
				makedirs(listsDir)
			path = join(listsDir, listKey + ".json")
			atomicWriteJson(path, data)
			atomicWriteJson(join(self.metadataDir, "last_import.json"), data)
			return path

		def writeReport(report, logMessages, startedAt):
			def pruneImportReports(keepFiles=40):
				files = []
				try:
					names = listdir(self.logsDir)
				except OSError:
					return
				for name in names:
					if not name.startswith("import-") or not name.endswith((".log", ".json")):
						continue
					path = join(self.logsDir, name)
					try:
						files.append((getmtime(path), path))
					except OSError:
						pass
				for unusedMtime, path in sorted(files, reverse=True)[keepFiles:]:
					try:
						unlink(path)
					except OSError:
						pass

			timestamp = strftime("%Y%m%d-%H%M%S", localtime(startedAt))
			sourceDigest = sha1(report.get("source_url", "").encode("utf-8")).hexdigest()[:8]
			baseName = "import-{0}-{1}".format(timestamp, sourceDigest)
			textPath = join(self.logsDir, baseName + ".log")
			jsonPath = join(self.logsDir, baseName + ".json")
			lastTextPath = join(self.logsDir, "last_import.log")
			lastJsonPath = join(self.logsDir, "last_import.json")

			lines = []
			lines.append("DVB-I Manager import report\n")
			lines.append("===========================\n")
			lines.append("List: {0}\n".format(report.get("service_list_name", "")))
			lines.append("Source: {0}\n".format(report.get("source_url", "")))
			lines.append("Country/Language/Region: {0}/{1}/{2}\n".format(report.get("country", ""), report.get("language", ""), report.get("region", "")))
			lines.append("HTTP status: {0}, changed: {1}\n".format(report.get("http_status", ""), report.get("http_changed", "")))
			lines.append("\nOptions\n")
			for key in sorted(report.get("options", {}).keys()):
				lines.append("- {0}: {1}\n".format(key, report["options"].get(key)))
			lines.append("\nStats\n")
			for key in [
				"total",
				"written",
				"written_tv",
				"written_radio",
				"skipped",
				"matched_broadcast",
				"written_broadcast",
				"written_ip",
				"drm_marked",
				"hbbtv_marked",
				"no_supported_instance",
				"logos_cached",
				"manifests_inspected",
				"manifests_clear",
				"manifests_drm",
				"content_guide_sources",
				"epg_services_mapped",
				"epg_services_with_schedule_endpoint",
				"xmltv_channels",
				"epg_probe_services",
				"epg_events_ready",
				"epg_services_synchronised",
				"epg_services_failed",
			]:
				lines.append("- {0}: {1}\n".format(key, report.get("stats", {}).get(key, 0)))
			lines.append("\nStatus counts\n")
			for key in sorted(report.get("status_counts", {}).keys()):
				lines.append("- {0}: {1}\n".format(key, report["status_counts"].get(key)))
			lines.append("\nLog\n")
			for message in logMessages:
				lines.append("- {0}\n".format(message))
			lines.append("\nServices\n")
			for item in report.get("services", []):
				lines.append(
					"- {0} | written={1} | kind={2} | type={3} | status={4} | lcn={5} | broadcast_match={6} | ip={7} | drm={8} | hbbtv={9}\n".format(
						item.get("name", ""),
						item.get("written", False),
						item.get("service_kind", item.get("service_media_kind", "tv")),
						item.get("selected_instance_type", ""),
						item.get("status", ""),
						item.get("lcn", ""),
						item.get("has_broadcast_match", False),
						item.get("has_ip", False),
						item.get("has_drm", False),
						item.get("has_hbbtv", False),
					)
				)
			lines.append("\nEPG mapping\n")
			lines.append("- ContentGuide metadata: {0}\n".format(report.get("epg", {}).get("content_guide_path", "")))
			lines.append("- Service map: {0}\n".format(report.get("epg", {}).get("service_map_path", "")))
			lines.append("- XMLTV test export: {0}\n".format(report.get("epg", {}).get("xmltv_path", "")))
			lines.append("- Endpoint probe JSON: {0}\n".format(report.get("epg", {}).get("epg_probe_path", "")))

			content = "".join(lines)
			atomicWrite(textPath, content)
			atomicWrite(lastTextPath, content)
			atomicWriteJson(jsonPath, report)
			atomicWriteJson(lastJsonPath, report)
			pruneImportReports(keepFiles=40)

			return textPath

		def buildReport(serviceList, fetchResult, bouquetResult, matched, logoCount, ipServiceType, piconDirs, installPicons, includeIp, includeDrm, preferBroadcast, showOtherRegions, epgResult, exportXmltv, exportEpgProbe, manifestReport):
			statusCounts = {}
			for item in bouquetResult.get("services", []):
				status = item.get("status") or "unknown"
				statusCounts[status] = statusCounts.get(status, 0) + 1

			return {
				"updated_at": int(time()),
				"service_list_name": serviceList.name,
				"source_url": serviceList.sourceUrl,
				"country": serviceList.country,
				"language": serviceList.language,
				"region": serviceList.region,
				"http_status": fetchResult.status,
				"http_changed": fetchResult.changed,
				"http_stale": bool(getattr(fetchResult, "stale", False)),
				"service_list_id": getattr(serviceList, "listId", ""),
				"service_list_version": getattr(serviceList, "version", ""),
				"options": {
					"include_ip": bool(includeIp),
					"include_drm": bool(includeDrm),
					"prefer_broadcast": bool(preferBroadcast),
					"show_other_regions": bool(showOtherRegions),
					"ip_service_type": str(ipServiceType),
					"install_picons": bool(installPicons),
					"picon_dirs": piconDirs,
					"export_xmltv": bool(exportXmltv),
					"export_epg_probe": bool(exportEpgProbe),
					"sync_epg_now_next": bool(epgResult.get("sync", {}).get("enabled")),
				},
				"stats": {
					"total": len(serviceList.services),
					"written": bouquetResult.get("added", 0),
					"written_tv": bouquetResult.get("services_written_tv", 0),
					"written_radio": bouquetResult.get("services_written_radio", 0),
					"skipped": bouquetResult.get("skipped", 0),
					"matched_broadcast": matched,
					"written_broadcast": bouquetResult.get("broadcast_written", 0),
					"written_ip": bouquetResult.get("ip_written", 0),
					"drm_marked": bouquetResult.get("drm_marked", 0),
					"hbbtv_marked": bouquetResult.get("hbbtv_marked", 0),
					"no_supported_instance": bouquetResult.get("no_supported_instance", 0),
					"logos_cached": logoCount,
					"manifests_inspected": manifestReport.get("scheduled_manifests", 0),
					"manifests_clear": manifestReport.get("counts", {}).get("clear", 0),
					"manifests_drm": manifestReport.get("counts", {}).get("drm_required", 0),
					"content_guide_sources": epgResult.get("stats", {}).get("content_guide_sources_total", 0),
					"epg_services_mapped": epgResult.get("stats", {}).get("services_with_content_guide", 0),
					"epg_services_with_schedule_endpoint": epgResult.get("stats", {}).get("services_with_schedule_endpoint", 0),
					"xmltv_channels": epgResult.get("stats", {}).get("xmltv_channels", 0),
					"epg_probe_services": epgResult.get("stats", {}).get("epg_probe_services", 0),
					"epg_events_ready": epgResult.get("sync", {}).get("events_total", 0),
					"epg_services_synchronised": epgResult.get("sync", {}).get("services_synchronised", 0),
					"epg_services_failed": epgResult.get("sync", {}).get("services_failed", 0),
					"epg_services_truncated": epgResult.get("sync", {}).get("services_truncated", 0),
				},
				"epg": epgResult,
				"manifest_inspection": manifestReport,
				"status_counts": statusCounts,
				"bouquet": {
					"unchanged": bouquetResult.get("unchanged", False),
					"bouquets_tv_changed": bouquetResult.get("bouquets_tv_changed", False),
					"bouquets_radio_changed": bouquetResult.get("bouquets_radio_changed", False),
					"tv": bouquetResult.get("bouquets", {}).get("tv", {}),
					"radio": bouquetResult.get("bouquets", {}).get("radio", {}),
				},
				"services": bouquetResult.get("services", []),
			}

		def removeStalePlaybackTokens():
			for stalePath in self.stalePlaybackTokenPaths:
				try:
					unlink(stalePath)
				except OSError:
					pass
			self.stalePlaybackTokenPaths = []

		def updatePlaybackMap(serviceList, includeDrm=False):
			"""Merge stable DVB-I playback targets for the WHERE_PLAYSERVICE hook."""
			path = join(self.metadataDir, "playback_map.json")
			playbackDir = join(self.metadataDir, "playback")
			listIndexDir = join(playbackDir, "lists")
			for directory in (playbackDir, listIndexDir):
				if not pathExists(directory):
					makedirs(directory)

			data = readJson(path, default={})
			services = data.get("services") if isinstance(data, dict) else None
			references = data.get("references") if isinstance(data, dict) else None
			if not isinstance(services, dict):
				services = {}
			if not isinstance(references, dict):
				references = {}

			sourceUrl = serviceList.sourceUrl
			listScope = getattr(serviceList, "listId", "") or sourceUrl
			listKey = sha1(listScope.encode("utf-8")).hexdigest()[:16]
			listIndexPath = join(listIndexDir, listKey + ".json")
			previousTokens = set(readJson(listIndexPath, default={}).get("tokens", []))
			currentTokens = []
			removeTokens = set(previousTokens)
			removeTokens.update(token for token, item in services.items() if item.get("source_url") == sourceUrl)
			for token in removeTokens:
				services.pop(token, None)
			for reference, token in list(references.items()):
				if token in removeTokens:
					del references[reference]

			for service in serviceList.services:
				if not service.selectedRef:
					continue
				serviceIdentity = service.dvbiId or "|".join([service.country, service.provider, service.name])
				identity = "|".join([listScope, serviceIdentity])
				token = sha256(identity.encode("utf-8")).hexdigest()[:32]
				currentTokens.append(token)
				selected = getattr(service, "selectedInstance", None)
				candidates = []
				for instance in service.instances:
					if instance.instanceType not in ("dash", "hls", "ip", "radio", "identifier", "rtsp", "multicast") or not instance.url:
						continue
					if getattr(instance, "playbackUnavailable", False):
						continue
					if getattr(instance, "drm", False) and not includeDrm:
						continue
					if not includeDrm and getattr(instance, "ftaVerified", True) is False:
						continue
					try:
						priority = int(getattr(instance, "priority", None))
					except Exception:
						priority = 0x7FFFFFFF
					candidates.append((priority, playbackUrl(instance)))
				candidates.sort(key=lambda item: item[0])
				urls = []
				if selected is not None and getattr(selected, "url", ""):
					urls.append(playbackUrl(selected))
				for priority, url in candidates:
					if url not in urls:
						urls.append(url)

				entry = {
					"token": token,
					"dvbi_id": service.dvbiId,
					"name": service.name,
					"provider": service.provider,
					"list_id": getattr(serviceList, "listId", ""),
					"source_url": sourceUrl,
					"enigma2_ref": service.selectedRef,
					"instance_type": service.selectedInstanceType,
					"media_kind": getattr(service, "selectedMediaKind", ""),
					"service_kind": getattr(service, "mediaKind", "tv"),
					"service_kind_source": getattr(service, "mediaKindSource", "default_tv"),
					"service_type_uri": getattr(service, "serviceTypeUri", ""),
					"service_type_term": getattr(service, "serviceTypeTerm", ""),
					"service_type": getattr(service, "serviceTypeTerm", ""),
					"player_type": getattr(service, "selectedPlayerType", "") or ("1" if service.selectedInstanceType == "broadcast" else ""),
					"url": urls[0] if urls else "",
					"fallback_urls": urls[1:],
					"updated_at": int(time()),
				}
				services[token] = entry
				references[service.selectedRef] = token
				atomicWriteJson(join(playbackDir, token + ".json"), entry)

			atomicWriteJson(
				listIndexPath,
				{
					"list_id": getattr(serviceList, "listId", ""),
					"source_url": sourceUrl,
					"tokens": currentTokens,
					"updated_at": int(time()),
				},
			)
			self.stalePlaybackTokenPaths = []
			for staleToken in previousTokens.difference(currentTokens):
				self.stalePlaybackTokenPaths.append(join(playbackDir, staleToken + ".json"))

			atomicWriteJson(
				path,
				{
					"version": 2,
					"updated_at": int(time()),
					"services": services,
					"references": references,
				},
			)
			return path

		options = options or {}
		country = options.get("country", "")
		language = options.get("language", "")
		region = options.get("region", "")
		showOtherRegions = bool(options.get("show_other_regions", True))
		includeIp = bool(options.get("include_ip", True))
		includeDrm = bool(options.get("include_drm", False))
		preferBroadcast = bool(options.get("prefer_broadcast", True))
		createBouquets = bool(options.get("create_bouquets", True))
		ipServiceType = str(options.get("ip_service_type", "4097") or "4097")
		downloadLogos = bool(options.get("download_logos", True))
		installPicons = bool(options.get("install_picons", True))
		if self.usingFlashFallback:
			downloadLogos = False
			installPicons = False
		exportXmltv = bool(options.get("export_xmltv", False))
		exportEpgProbe = bool(options.get("export_epg_probe", True))
		syncEpgNowNext = bool(options.get("sync_epg_now_next", False))
		epgWorkers = int(options.get("epg_workers", 4) or 4)
		inspectManifests = bool(options.get("inspect_manifests", False))
		manifestMax = int(options.get("manifest_max", 256) or 256)
		probeMedia = bool(options.get("probe_media", inspectManifests))
		mediaProbeMax = int(options.get("media_probe_max", manifestMax) or manifestMax)
		xmltvPath = options.get("xmltv_path", "")
		# Only use existing E2-selected destinations; never create or substitute a picon directory.
		piconDirs = [path for path in options.get("picon_dirs", []) if isdir(path) and access(path, W_OK)]
		downloadLogos = downloadLogos and bool(piconDirs)
		installPicons = installPicons and bool(piconDirs)
		force = bool(options.get("force", False))
		bouquetKey = options.get("bouquet_key", "")
		serviceSnapshot = options.get("service_snapshot", [])
		requireFtaVerification = bool(options.get("require_fta_verification", False))
		reloadBouquets = bool(options.get("reload_bouquets", True))

		logMessages = []
		startedAt = int(time())

		def log(message):
			logMessages.append("{0} {1}".format(strftime("%Y-%m-%d %H:%M:%S"), message))
			self.logger(message)

		log(_("Fetching DVB-I service list: {0}").format(url))
		fetcher = ServiceListFetcher(self.httpCacheDir)
		fetchResult = fetcher.fetch(url, force=force)

		log(_("Parsing DVB-I service list"))
		parser = ServiceListParser()

		def parseList(content):
			parsed = parser.parse(
				content,
				sourceUrl=url,
				country=country,
				language=language,
				region=region,
			)
			responseStatus = (getattr(parsed, "responseStatus", "") or "OK").upper()
			if responseStatus != "OK":
				raise ValueError(_("DVB-I service list responseStatus is {0}").format(responseStatus))
			if not parsed.services:
				raise ValueError(_("DVB-I service list contains no services"))
			return parsed

		lastGood = fetcher.readLastGood(url)
		try:
			serviceList = parseList(fetchResult.content)
			if lastGood and lastGood != fetchResult.content:
				previousList = parseList(lastGood)
				previousCount = len(previousList.services)
				currentCount = len(serviceList.services)
				if previousCount >= 10 and currentCount * 2 < previousCount and not options.get("allow_large_removal", False):
					raise ValueError(_("service count dropped from {0} to {1}; refusing an unconfirmed mass removal").format(previousCount, currentCount))
			fetcher.markGood(url, fetchResult.content)
		except Exception as parseError:
			if not lastGood or lastGood == fetchResult.content:
				raise
			log(_("Current service list is invalid; using last-known-good data: {0}").format(parseError))
			serviceList = parseList(lastGood)
			fetchResult.stale = True

		if not bouquetKey:
			listIdentity = getattr(serviceList, "listId", "") or url
			# One provider can serve several regions under the same ListId.
			# Keep IP-only test lists separate from broadcast/hybrid bouquets.
			delivery = "hybrid" if preferBroadcast and includeIp else "broadcast" if preferBroadcast else "ip"
			identity = jsonDumps([listIdentity, serviceList.region or region, delivery], ensure_ascii=True)
			digest = sha1(identity.encode("utf-8")).hexdigest()[:12]
			bouquetKey = "{0}_{1}".format((country or "global").lower(), digest)

		scopedServices = [service for service in serviceList.services if not serviceVisibilityStatus(service, region, showOtherRegions)]
		manifestReport = {
			"eligible_instances": 0,
			"scheduled_manifests": 0,
			"services_out_of_scope": len(serviceList.services) - len(scopedServices),
			"counts": {},
			"results": [],
		}
		mediaProbeReport = {
			"eligible_instances": 0,
			"scheduled_urls": 0,
			"counts": {},
			"results": [],
		}
		if probeMedia:
			log(_("Detecting IP media types with bounded range probes"))
			mediaProbeReport = MediaProbe(
				maxUrls=mediaProbeMax,
				maxWorkers=4,
			).inspect(scopedServices)

		for service in serviceList.services:
			for instance in service.instances:
				kind = effectiveMediaKind(instance)
				if not probeMedia and instance.instanceType not in ("dash", "hls"):
					clearNonAdaptive = True
				else:
					clearNonAdaptive = kind in (
						"mpeg-ts",
						"progressive",
						"radio",
						"rtsp",
						"multicast-ts",
					)
				instance.ftaVerified = bool(not instance.drm and clearNonAdaptive)
		if inspectManifests:
			log(_("Inspecting adaptive manifests for FTA protection"))
			inspector = ManifestInspector(
				cacheDir=self.httpCacheDir,
				maxManifests=manifestMax,
				maxWorkers=4,
			)
			manifestReport = inspector.inspect(scopedServices)
			manifestReport["services_out_of_scope"] = len(serviceList.services) - len(scopedServices)
			for service in serviceList.services:
				for instance in service.instances:
					if effectiveMediaKind(instance) in ("dash", "hls"):
						instance.ftaVerified = bool(not instance.drm and getattr(instance, "manifestProbeStatus", "") == "clear")
		manifestReport["media_probe"] = mediaProbeReport

		log(_("Matching broadcast services"))
		matcher = ServiceMatcher(
			join(self.enigma2Dir, "lamedb"),
			serviceSnapshot=serviceSnapshot,
			requireFtaVerification=requireFtaVerification,
		)
		matched = 0
		for service in serviceList.services:
			ref = matcher.matchService(service, includeDrm=includeDrm)
			if ref:
				service.matchedRef = ref
				matched += 1
				if preferBroadcast:
					service.selectedRef = ref
					service.selectedInstanceType = "broadcast"
					service.selectedInstance = getattr(service, "matchedInstance", None)
					service.status = "matched_broadcast"
				else:
					service.status = "broadcast_match_available"

		log(_("Preparing channel lists and fallback mappings"))
		writer = BouquetWriter(self.enigma2Dir)
		bouquetResult = writer.write(
			serviceList,
			bouquetKey=bouquetKey,
			includeIp=includeIp,
			includeDrm=includeDrm,
			ipServiceType=ipServiceType,
			preferBroadcast=preferBroadcast,
			reloadBouquets=reloadBouquets,
			commit=False,
			requireFtaVerification=requireFtaVerification,
			showOtherRegions=showOtherRegions,
			availablePlayers=options.get("available_players", ()),
			automaticPlayer=bool(options.get("automatic_player", True)),
			hybrid=bool(options.get("hybrid", False)),
			labelVod=bool(options.get("label_vod", False)),
		)
		if createBouquets and options.get("native_bouquets") and not bouquetResult["added"]:
			raise ValueError(
				_("No playable free-to-air channels found. Check reception, region and installed players. Existing channel lists were not changed.")
			)
		logoCount = 0
		if downloadLogos:
			log(_("Caching DVB-I logos"))
			logoCache = LogoCache(self.logoCacheDir, installPicons=installPicons, piconDirs=piconDirs)
			logoCount = logoCache.cacheServiceLogos([service for service in serviceList.services if service.selectedRef])

		log(_("Writing DVB-I ContentGuide / EPG mapping metadata"))
		regionMetadataPath = self.writeRegionCatalog(serviceList)
		storedRegionCatalog = readJson(regionMetadataPath, default={}).get("regions", [])
		epgBridge = EpgBridge(self.metadataDir)
		epgResult = epgBridge.writeSources(serviceList, exportXmltv=exportXmltv, xmltvPath=xmltvPath, exportProbe=exportEpgProbe)
		epgSyncResult = {
			"entries": [],
			"enabled": syncEpgNowNext,
			"services_considered": 0,
			"services_eligible": 0,
			"services_truncated": 0,
			"services_synchronised": 0,
			"services_failed": 0,
			"events_total": 0,
		}
		if syncEpgNowNext:
			log(_("Synchronising DVB-I now/next EPG"))
			epgSync = EpgSynchronizer(
				self.httpCacheDir,
				maxWorkers=epgWorkers,
			)
			epgSyncResult = epgSync.syncNowNext(serviceList, force=force)
		epgEvents = epgSyncResult.pop("entries", [])
		epgResult["sync"] = epgSyncResult
		playbackMapPath = updatePlaybackMap(serviceList, includeDrm=includeDrm)

		nativeBouquets = None
		if createBouquets and options.get("native_bouquets"):
			log(_("Preparing channel lists for Enigma2"))
			nativeBouquets = writer.nativePayload(bouquetResult)
			# Old tokens must remain resolvable if the subsequent native
			# handoff fails. They can be garbage-collected after a successful
			# receiver-side commit; never unlink them before the main-loop commit.
		elif createBouquets:
			log(_("Committing DVB-I bouquet"))
			bouquetResult = writer.commitPrepared(bouquetResult)
			removeStalePlaybackTokens()
		bouquetResult["commit_state"] = ("prepared" if nativeBouquets is not None else "committed") if createBouquets else "not_requested"
		if not createBouquets:
			for field in ("added", "services_written_tv", "services_written_radio", "broadcast_written", "ip_written"):
				bouquetResult[field] = 0

		report = buildReport(
			serviceList,
			fetchResult,
			bouquetResult,
			matched,
			logoCount,
			ipServiceType,
			piconDirs,
			installPicons,
			includeIp,
			includeDrm,
			preferBroadcast,
			showOtherRegions,
			epgResult,
			exportXmltv,
			exportEpgProbe,
			manifestReport,
		)
		try:
			reportPath = writeReport(report, logMessages, startedAt)
		except Exception as error:
			reportPath = ""
			log(_("Could not write optional import report: {0}").format(error))
		try:
			metadataPath = writeMetadata(
				serviceList,
				fetchResult,
				bouquetResult,
				matched,
				logoCount,
				epgResult,
				ipServiceType,
				piconDirs,
				installPicons,
				reportPath,
				report,
				preferBroadcast,
				exportXmltv,
			)
		except Exception as error:
			metadataPath = ""
			log(_("Could not write optional import metadata: {0}").format(error))

		result = {
			"create_bouquets": createBouquets,
			# Independent of bouquet mode: changing IP/broadcast preference must
			# update the same provider/region mapping rather than leave stale pairs.
			"fallback_scope": jsonDumps([getattr(serviceList, "listId", "") or baseServiceListUrl(url), serviceList.region or region], ensure_ascii=True),
			"hybrid_services": bouquetResult.get("hybrid_services", []),
			"service_list_name": serviceList.name,
			"source_url": url,
			"services_total": len(serviceList.services),
			"services_matched_broadcast": matched,
			"services_written": bouquetResult.get("added", 0),
			"services_written_tv": bouquetResult.get("services_written_tv", 0),
			"services_written_radio": bouquetResult.get("services_written_radio", 0),
			"services_skipped": bouquetResult.get("skipped", 0),
			"services_written_broadcast": bouquetResult.get("broadcast_written", 0),
			"services_written_ip": bouquetResult.get("ip_written", 0),
			"services_drm_marked": bouquetResult.get("drm_marked", 0),
			"services_hbbtv_marked": bouquetResult.get("hbbtv_marked", 0),
			"regions_available": len(storedRegionCatalog),
			"region_catalog_path": regionMetadataPath,
			"manifests_inspected": manifestReport.get("scheduled_manifests", 0),
			"manifests_clear": manifestReport.get("counts", {}).get("clear", 0),
			"manifests_drm": manifestReport.get("counts", {}).get("drm_required", 0),
			"media_urls_probed": mediaProbeReport.get("scheduled_urls", 0),
			"media_types_confirmed": mediaProbeReport.get("counts", {}).get("confirmed", 0),
			"ip_service_type": ipServiceType,
			"prefer_broadcast": preferBroadcast,
			"logos_cached": logoCount,
			"bouquet_tv": bouquetResult.get("bouquet_tv", ""),
			"bouquet_tv_path": bouquetResult.get("bouquet_tv_path", ""),
			"bouquet_radio": bouquetResult.get("bouquet_radio", ""),
			"bouquet_radio_path": bouquetResult.get("bouquet_radio_path", ""),
			"bouquet_unchanged": bouquetResult.get("unchanged", False),
			"bouquets_tv_changed": bouquetResult.get("bouquets_tv_changed", False),
			"bouquets_radio_changed": bouquetResult.get("bouquets_radio_changed", False),
			"metadata_path": metadataPath,
			"epg_metadata_path": epgResult.get("content_guide_path", ""),
			"epg_service_map_path": epgResult.get("service_map_path", ""),
			"epg_xmltv_path": epgResult.get("xmltv_path", ""),
			"epg_probe_path": epgResult.get("epg_probe_path", ""),
			"epg_sources_total": epgResult.get("stats", {}).get("content_guide_sources_total", 0),
			"epg_services_mapped": epgResult.get("stats", {}).get("services_with_content_guide", 0),
			"epg_services_with_schedule_endpoint": epgResult.get("stats", {}).get("services_with_schedule_endpoint", 0),
			"epg_probe_services": epgResult.get("stats", {}).get("epg_probe_services", 0),
			"epg_events": epgEvents,
			"epg_events_ready": epgSyncResult.get("events_total", 0),
			"epg_services_synchronised": epgSyncResult.get("services_synchronised", 0),
			"epg_services_failed": epgSyncResult.get("services_failed", 0),
			"epg_services_truncated": epgSyncResult.get("services_truncated", 0),
			"report_path": reportPath,
			"http_changed": fetchResult.changed,
			"http_status": fetchResult.status,
			"http_stale": bool(getattr(fetchResult, "stale", False)),
			"playback_map_path": playbackMapPath,
		}
		if nativeBouquets is not None:
			result["native_bouquets"] = nativeBouquets
			result["bouquets_committed"] = False
		if createBouquets:
			log(_("Channel list ready: {0} TV, {1} radio").format(result["services_written_tv"], result["services_written_radio"]))
		else:
			log(_("Fallback mappings prepared: {0}").format(len(result["hybrid_services"])))
		return result

	def discoverRegions(self, url, options=None):
		"""Fetch one list and persist its selectable RegionList without importing it."""
		options = options or {}
		url = baseServiceListUrl(url)
		fetcher = ServiceListFetcher(self.httpCacheDir)
		fetchResult = fetcher.fetch(url, force=bool(options.get("force", False)))
		parser = ServiceListParser()

		def parse(content):
			serviceList = parser.parse(
				content,
				sourceUrl=url,
				country=options.get("country", ""),
				language=options.get("language", ""),
			)
			responseStatus = (getattr(serviceList, "responseStatus", "") or "OK").upper()
			if responseStatus != "OK":
				raise ValueError(_("DVB-I service list responseStatus is {0}").format(responseStatus))
			return serviceList

		lastGood = fetcher.readLastGood(url)
		try:
			serviceList = parse(fetchResult.content)
			fetcher.markGood(url, fetchResult.content)
		except Exception:
			if not lastGood or lastGood == fetchResult.content:
				raise
			serviceList = parse(lastGood)
			fetchResult.stale = True

		path = self.writeRegionCatalog(serviceList, preserveExisting=False)
		selectable = [item for item in getattr(serviceList, "regionCatalog", []) if item.get("selectable", True)]
		return {
			"service_list_name": serviceList.name,
			"source_url": url,
			"region_catalog_path": path,
			"regions_total": len(getattr(serviceList, "regionCatalog", [])),
			"regions_selectable": len(selectable),
			"http_status": fetchResult.status,
			"http_stale": bool(getattr(fetchResult, "stale", False)),
		}

	def writeRegionCatalog(self, serviceList, preserveExisting=True):
		path = regionCatalogPath(self.metadataDir, serviceList.sourceUrl)
		regions = list(getattr(serviceList, "regionCatalog", []))
		version = getattr(serviceList, "regionCatalogVersion", None)
		language = getattr(serviceList, "regionCatalogLanguage", "")
		if preserveExisting:
			previousData = readJson(path, default={})
			previous = previousData.get("regions", [])
			if isinstance(previous, list) and len(previous) > len(regions):
				# A region-specific response may omit the full RegionList.
				# Keep the richer base-list discovery until it is explicitly
				# refreshed by discover_regions().
				regions = previous
				version = previousData.get("version")
				language = previousData.get("language", "")
		atomicWriteJson(
			path,
			{
				"schema": "org.openatv.dvbi.region-catalog.v1",
				"updated_at": int(time()),
				"service_list_id": getattr(serviceList, "listId", ""),
				"service_list_name": serviceList.name,
				"source_url": baseServiceListUrl(serviceList.sourceUrl),
				"version": version,
				"language": language,
				"regions": regions,
			},
		)
		return path

	def loadLastImport(self):
		path = join(self.metadataDir, "last_import.json")
		if not pathExists(path):
			return {}
		return readJson(path, default={})


SYSTEM_SOURCES_PATH = "/etc/tuxbox/dvbi.xml"
USER_SOURCES_PATH = "/etc/enigma2/dvbi.xml"


def loadSourceConfig(userPath=USER_SOURCES_PATH, systemPath=None):
	"""User XML replaces system sources; broken overrides never fall back silently."""
	path = userPath if userPath and pathExists(userPath) else (systemPath or SYSTEM_SOURCES_PATH)
	try:
		with open(path, "rb") as handle:
			content = handle.read(262145)
		if len(content) > 262144 or b"<!DOCTYPE" in content.upper() or b"<!ENTITY" in content.upper():
			raise ValueError(_("oversized XML or unsupported document declaration"))
		root = fromstring(content)
		if root.tag != "dvbi":
			raise ValueError(_("expected <dvbi> root"))
		sources, named = [], {}
		for node in root:
			if node.tag != "source":
				raise ValueError(_("expected <source> entry"))
			url = node.get("url", "").strip()
			parsed = urlsplit(url)
			if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None:
				raise ValueError(_("source URL must be an absolute HTTP(S) URL without credentials"))
			source = {"url": url}
			for key in ("kind", "country", "target_country"):
				if node.get(key):
					source[key] = node.get(key).strip()
			if source.get("kind", "registry") not in ("registry", "service_list"):
				raise ValueError(_("invalid source kind"))
			for key in ("optional", "regulator_only", "unregulated_test", "test_source"):
				if node.get(key) is not None:
					value = node.get(key).strip().lower()
					if value not in ("true", "false", "1", "0"):
						raise ValueError(_("invalid boolean: ") + key)
					source[key] = value in ("true", "1")
			if node.get("id"):
				if node.get("id") in named:
					raise ValueError(_("duplicate source id"))
				named[node.get("id")] = url
			sources.append(source)
		if not sources:
			raise ValueError(_("no discovery sources configured"))
		defaultId = root.get("defaultSource", "")
		if defaultId and defaultId not in named:
			raise ValueError(_("unknown defaultSource"))
		return {
			"path": path,
			"sources": sources,
			"named_sources": named,
			"default_country": root.get("defaultCountry", ""),
			"default_region": root.get("defaultRegion", ""),
			"default_registry": named.get(defaultId, ""),
		}
	except (OSError, ParseError, ValueError) as error:
		raise ValueError(_("Invalid DVB-I source configuration {0}: {1}").format(path, error)) from error


REGISTRY_CATALOG_SCHEMA = "org.openatv.dvbi.registry-catalog.v1"
REGISTRY_CATALOG_TTL = 24 * 60 * 60
MAX_REGISTRY_LISTS = 2048


def getRegistrySourceList(endpoint=None):
	sources = loadSourceConfig()["sources"]
	if endpoint and endpoint.rstrip("/").casefold() != sources[0]["url"].rstrip("/").casefold():
		return [{"url": endpoint}]
	return sources


def publicListUrl(url):
	"""Ignore broken and local-only entries in public discovery catalogues."""
	try:
		parsed = urlsplit(url)
		host = (parsed.hostname or "").rstrip(".").lower()
		if parsed.scheme not in ("http", "https") or not host or parsed.username is not None:
			return False
		if host == "localhost" or host.endswith((".localhost", ".local")) or "." not in host and ":" not in host:
			return False
		try:
			return ipAddress(host).is_global
		except ValueError:
			return True
	except (ValueError, TypeError):
		return False


def directOffering(client, source, force=False):
	response = client.fetcher.fetch(source["url"], force=force)
	listing = ServiceListParser().parse(response.content, sourceUrl=source["url"], country=source.get("country", ""))
	if not listing.services:
		raise ValueError(_("service list contains no services"))
	deliveryNames = {
		"dash": "DASHDelivery",
		"dvb-t": "DVBTDelivery",
		"dvb-s": "DVBSDelivery",
		"dvb-c": "DVBCDelivery",
		"hls": "OtherDelivery",
		"ip": "OtherDelivery",
		"application": "ApplicationDelivery",
	}
	return [
		{
			"id": listing.listId,
			"name": listing.name,
			"provider": listing.provider,
			"url": source["url"],
			"urls": [source["url"]],
			"target_countries": [listing.country] if listing.country else [],
			"languages": [listing.language] if listing.language else [],
			"delivery": sorted(
				{deliveryNames[item.instanceType] for service in listing.services for item in service.instances if item.instanceType in deliveryNames}
			),
			"registry_cache_stale": response.stale,
			"service_count": len(listing.services),
		}
	]


def ftaOptions(rawOptions):
	"""Enforce the phase-1 policy in the shared task, not only in the UI."""
	options = dict(rawOptions or {})
	options["include_drm"] = False
	options["probe_media"] = True
	options["inspect_manifests"] = True
	options["require_fta_verification"] = True
	return options


def sortedUniqueStrings(values):
	"""Return clean, case-insensitively unique strings in stable display order."""
	cleaned = sorted(
		{value.strip() for value in values if isinstance(value, str) and value.strip()},
		key=lambda value: (value.casefold(), value),
	)
	result = []
	seen = set()
	for value in cleaned:
		identity = value.casefold()
		if identity not in seen:
			seen.add(identity)
			result.append(value)
	return result


def offeringValues(offering, field):
	value = offering.get(field)
	if isinstance(value, str):
		return [value]
	if isinstance(value, (list, tuple, set)):
		return list(value)
	return []


def catalogFacets(offerings):
	"""Build selectable registry dimensions without relying on UI state."""
	providers = []
	languages = []
	countries = []
	delivery = []
	for offering in offerings or []:
		if not isinstance(offering, dict):
			continue
		providers.extend(offeringValues(offering, "provider"))
		languages.extend(offeringValues(offering, "languages"))
		countries.extend(offeringValues(offering, "target_countries"))
		delivery.extend(offeringValues(offering, "delivery"))
	return {
		"providers": sortedUniqueStrings(providers),
		"languages": sortedUniqueStrings(languages),
		"target_countries": sortedUniqueStrings(countries),
		"delivery": sortedUniqueStrings(delivery),
	}


def saveCatalog(manager, endpoint, offerings, force=False, catalogDir="", registrySources=None, sourceErrors=None):
	"""Publish a complete live catalogue without destroying last-known-good data."""
	metadataDir = catalogDir or manager.metadataDir
	if not pathExists(metadataDir):
		makedirs(metadataDir, exist_ok=True)
	path = join(metadataDir, "registry_catalog.json")
	if not offerings:
		raise ValueError(_("live registry returned no service lists; keeping the cached catalogue"))
	previous = readJson(path, default={})
	previousOfferings = previous.get("offerings", []) if isinstance(previous, dict) else []
	if not force and isinstance(previousOfferings, list) and len(previousOfferings) >= 10 and len(offerings) * 2 < len(previousOfferings):
		raise ValueError(_("live registry shrank from {0} to {1} lists; keeping last-known-good data").format(len(previousOfferings), len(offerings)))
	updatedAt = int(time())
	atomicWriteJson(
		path,
		{
			"schema": REGISTRY_CATALOG_SCHEMA,
			"query_scope": "all",
			"updated_at": updatedAt,
			"expires_at": updatedAt + (300 if sourceErrors else REGISTRY_CATALOG_TTL),
			"registry_url": endpoint,
			"registry_sources": registrySources or [{"url": endpoint}],
			"source_errors": sourceErrors or [],
			"offering_count": len(offerings),
			"facets": catalogFacets(offerings),
			"offerings": offerings,
		},
	)
	return path


def discover(manager, job):
	defaultEndpoint = loadSourceConfig()["sources"][0]["url"]
	endpoint = job.get("registry_url") or defaultEndpoint
	client = CsrClient(manager.httpCacheDir)
	sources = job.get("registry_sources") or [{"url": endpoint}]
	if not isinstance(sources, list) or len(sources) > 8:
		raise ValueError(_("invalid registry source set"))
	previous = readJson(join(job.get("catalog_dir") or getattr(manager, "metadataDir", ""), "registry_catalog.json"), default={})
	previousItems = previous.get("offerings", []) if isinstance(previous, dict) else []
	if not isinstance(previousItems, list):
		previousItems = []
	previousItems = [item for item in previousItems if isinstance(item, dict)]
	previousKeys = {item.get("url"): item.get("selection_id") or item.get("id") or item.get("url") for item in previousItems}
	offerings = []
	seen = set()
	errors = []
	for source in sources:
		getattr(manager, "logger", lambda message: None)(_("Discovering lists: {0}").format(source["url"]))
		try:
			if source.get("kind") == "service_list":
				found = directOffering(client, source, force=bool(job.get("force", False)))
			else:
				found = client.query(
					source["url"],
					targetCountry=source.get("target_country", ""),
					force=bool(job.get("force", False)),
				)
			if source.get("regulator_only"):
				found = [item for item in found if item.get("regulator_list")]
			if not found or any(item.get("registry_cache_stale") for item in found):
				raise ValueError(_("Source unavailable, stale or empty"))
		except Exception as error:
			if not source.get("optional"):
				raise
			errors.append({"url": source["url"], "error": str(error)})
			# An optional pilot going offline must not hide other lists, or erase
			# this source's last-known-good offerings. Never fabricate a fallback.
			found = [
				dict(item, registry_cache_stale=True) for item in previousItems if (item.get("discovery_source") or item.get("registry_url")) == source["url"]
			]
		for item in found:
			item = dict(item)
			urls = item.get("urls") or [item.get("url", "")]
			urls = [url for url in urls if publicListUrl(url)]
			if not urls:
				continue
			item.update(url=urls[0], urls=urls, discovery_source=source["url"])
			item["test_source"] = bool(source.get("test_source") or source.get("unregulated_test") and not item.get("regulator_list"))
			if source["url"].rstrip("/") == defaultEndpoint.rstrip("/"):
				# These sample offerings include livesim test pictures despite
				# production-looking names (verified September 2026).
				path = urlsplit(item["url"]).path.rstrip("/")
				words = (item.get("name", "").casefold() + " " + path.replace("/", " ")).split()
				item["test_source"] = bool(set(words) & {"test", "demo", "pilot", "example"} or path in ("/lists/aus/freeview", "/lists/irl/saorview"))
			identity = item["url"]
			if identity not in seen:
				seen.add(identity)
				offerings.append(item)
		if len(offerings) > MAX_REGISTRY_LISTS:
			raise ValueError(_("registry returned too many service lists"))
	# Different environments may reuse a list identifier. Never collapse their
	# selector values or accidentally resolve a production selection to a testbed.
	ids = {}
	for item in offerings:
		ids[item.get("id")] = ids.get(item.get("id"), 0) + 1
	for item in offerings:
		if item["url"] in previousKeys:
			item["selection_id"] = previousKeys[item["url"]]
		elif item.get("id") and ids[item["id"]] > 1:
			item["selection_id"] = item["id"] + "|" + sha256(item.get("url", "").encode("utf-8")).hexdigest()[:16]
	catalogPath = saveCatalog(
		manager,
		endpoint,
		offerings,
		force=bool(job.get("force", False)),
		catalogDir=str(job.get("catalog_dir") or ""),
		registrySources=sources,
		sourceErrors=errors,
	)
	return offerings, catalogPath, errors


def runSync(job, logger=None):
	dataDir = job.get("data_dir")
	enigma2Dir = job.get("enigma2_dir", "/etc/enigma2")
	manager = DvbIManager(dataDir=dataDir, enigma2Dir=enigma2Dir)
	if logger is not None:
		manager.logger = logger
	action = job.get("action", "import_url")

	if action == "discover_regions":
		result = manager.discoverRegions(job["url"], dict(job.get("options") or {}))
		result.update(
			{
				"action": action,
				"catalog_only": True,
				"sync_complete": True,
				"needs_bouquet_reload": False,
			}
		)
		return result

	if action == "import_url":
		options = ftaOptions(job.get("options"))
		options["reload_bouquets"] = False
		result = manager.importUrl(job["url"], options)
		result["action"] = action
		result["catalog_only"] = False
		result["sync_complete"] = not (result.get("http_stale") or result.get("epg_services_failed") or result.get("epg_services_truncated"))
		result["needs_bouquet_reload"] = result.get("create_bouquets", True) and (
			not result.get("bouquet_unchanged", False) or result.get("bouquets_tv_changed", False) or result.get("bouquets_radio_changed", False)
		)
		return result

	if action != "discover_registry":
		raise ValueError(_("unknown DVB-I task action: {0}").format(action))
	offerings, catalogPath, errors = discover(manager, job)
	return {
		"action": action,
		"catalog_only": True,
		"catalog_path": catalogPath,
		"registry_url": job.get("registry_url") or loadSourceConfig()["sources"][0]["url"],
		"offerings_total": len(offerings),
		"source_errors": errors,
		"sync_complete": not errors,
		"needs_bouquet_reload": False,
	}
