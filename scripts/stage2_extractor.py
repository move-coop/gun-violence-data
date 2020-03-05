import re

from bs4 import BeautifulSoup
from collections import defaultdict, namedtuple

from log_utils import log_first_call

Field = namedtuple('Field', ['name', 'value'])

ALL_FIELD_NAMES = sorted([
    'latitude',
    'longitude',
    'location_description',

    'participant_type',
    'participant_name',
    'participant_age',
    'participant_age_group',
    'participant_gender',
    'participant_status',
    'participant_relationship',

    'incident_characteristics',

    'notes',

    'n_guns_involved',
    'gun_type',
    'gun_stolen',

    'sources',

    'congressional_district',
    'state_senate_district',
    'state_house_district'
])

NIL_FIELDS = tuple([Field(name, None) for name in ALL_FIELD_NAMES])

def _find_div_with_title(title, soup):
    common_parent = soup.select_one('#block-system-main')
    header = common_parent.find('h2', string=title)
    return header.parent if header else None

def _out_name(in_name, prefix=''):
    return prefix + in_name.lower().replace(' ', '_') # e.g. 'Age Group' -> 'participant_age_group'

def _getdict(lines, apply=None):
    d = {}
    for line in lines:
        if not line:
            continue
        index = line.find(':')
        assert index != -1

        key, value = line[:index], line[index + 2:]
        assert key not in d
        if apply:
            value = apply(value)
        d[key] = value
    return d

def _getdicts(linegroups, apply=None):
    ds = {}
    groupno = 0
    for lines in linegroups:
        ds[groupno] = _getdict(lines, apply)
        groupno += 1
    # Swap the order of mappings. ds[0]['name'] -> ds['name'][0]
    ds2 = defaultdict(dict)
    for groupno, d in ds.items():
        for key, value in d.items():
            ds2[key][groupno] = value
    return ds2

def _normalize(fields):
    # Ensure that the fields are alphabetically ordered by field name.
    # Also, add dummy ('field_name', None) fields for missing field names.
    fields = sorted(fields, key=lambda f: f.name)

    field_names = set(next(zip(*fields)))
    should_be_empty = field_names - set(ALL_FIELD_NAMES)
    assert not should_be_empty, "We're missing these field names: {}".format(should_be_empty)

    i = 0
    for name in ALL_FIELD_NAMES:
        if name not in field_names:
            dummy = Field(name, None)
            fields.insert(i, dummy)
        i += 1

    assert len(fields) == len(ALL_FIELD_NAMES), "{} may have duplicate or missing fields".format(fields)
    # Since `fields` has a known length, it's more appropriate to use a tuple.
    return tuple(fields)

def _stringify_list(lst, sep='||'):
    violation = next((item for item in lst if sep in item), None)
    assert violation is None, "List item {} contains the separator string {}".format(repr(violation), repr(sep))
    return sep.join(lst)

def _stringify_dict(d, insep='::', outsep='||'):
    keys, values = list(map(str, d.keys())), list(map(str, d.values()))
    key_violation = next((key for key in keys if insep in key or outsep in key), None)
    value_violation = next((value for value in values if insep in value or outsep in value), None)
    assert key_violation is None and value_violation is None, \
           "Key {} or value {} contains the separator string(s) {} or {}".format(
               repr(key_violation), repr(value_violation), repr(insep), repr(outsep))

    return outsep.join([insep.join([k, v]) for k, v in zip(keys, values)])

class Stage2Extractor(object):
    def extract_fields(self, text, ctx):
        log_first_call()
        soup = BeautifulSoup(text, features='html')

        location_fields = self._extract_location_fields(soup, ctx)
        participant_fields = self._extract_participant_fields(soup)
        incident_characteristics = self._extract_incident_characteristics(soup)
        notes = self._extract_notes(soup)
        guns_involved_fields = self._extract_guns_involved_fields(soup)
        sources = self._extract_sources(soup)
        district_fields = self._extract_district_fields(soup)

        return _normalize([*location_fields,
                           *participant_fields,
                            Field('incident_characteristics', incident_characteristics),
                            Field('notes', notes),
                           *guns_involved_fields,
                            Field('sources', sources),
                           *district_fields])

    def _extract_location_fields(self, soup, ctx):
        def describes_city_and_state(line):
            return ',' in line and line.endswith(ctx.state) # and line.startswith(ctx.city_or_county)

        def describes_address(line):
            # The address on the incident page usually, but not always, matches the address on the query page.
            return line == ctx.address or \
                re.search(r'^[0-9]+[0-9a-z-]*\b', line, re.I) or \
                re.search(r'\b(st|street|rd|road|dr|drive|blvd|boulevard|ave|avenue|hwy|highway)\.?$', line, re.I)

        div = _find_div_with_title('Location', soup)
        if div is None:
            return

        for span in div.select('span'):
            text = span.text
            if not text:
                continue
            match = re.search(r'^Geolocation:\s+(.*),\s+(.*)$', text)
            if match:
                latitude, longitude = float(match.group(1)), float(match.group(2))
                yield Field('latitude', latitude)
                yield Field('longitude', longitude)
            elif describes_city_and_state(text) or describes_address(text):
                # Nothing to be done. City, state, and address fields are already included in the stage1 dataset.
                pass
            else:
                yield Field('location_description', text)

    def _extract_participant_fields(self, soup):
        div = _find_div_with_title('Participants', soup)
        if div is None:
            return

        linegroups = [[li.text for li in ul.select('li')] for ul in div.select('ul')]
        for field_name, field_values in _getdicts(linegroups).items():
            field_name = _out_name(field_name, prefix='participant_')
            field_values = _stringify_dict(field_values)
            yield Field(field_name, field_values)

    def _extract_incident_characteristics(self, soup):
        div = _find_div_with_title('Incident Characteristics', soup)
        return None if div is None else _stringify_list([li.text for li in div.select('li')])

    def _extract_notes(self, soup):
        div = _find_div_with_title('Notes', soup)
        return None if div is None else div.select_one('p').text

    def _extract_guns_involved_fields(self, soup):
        div = _find_div_with_title('Guns Involved', soup)
        if div is None:
            return

        # n_guns_involved
        p_text = div.select_one('p').text
        match = re.search(r'^([0-9]+)\s+guns?\s+involved.$', p_text)
        assert match, "<p> text did not match expected pattern: {}".format(p_text)
        n_guns_involved = int(match.group(1))
        yield Field('n_guns_involved', n_guns_involved)

        # List attributes
        linegroups = [[li.text for li in ul.select('li')] for ul in div.select('ul')]
        for field_name, field_values in _getdicts(linegroups).items():
            field_name = _out_name(field_name, prefix='gun_')
            field_values = _stringify_dict(field_values)
            yield Field(field_name, field_values)

    def _extract_sources(self, soup):
        div = _find_div_with_title('Sources', soup)
        if div is None:
            return None

        anchors = [a for a in div.select('a') if a.text == a['href']]
        return _stringify_list([a['href'] for a in anchors])

    def _extract_district_fields(self, soup):
        div = _find_div_with_title('District', soup)
        if div is None:
            return

        # The text we want to scrape is orphaned (no direct parent element), so we can't get at it directly.
        # Fortunately, each important line is followed by a <br> element, so we can use that to our advantage.
        # NB: The orphaned text elements are of type 'NavigableString'
        lines = [str(br.previousSibling).strip() for br in div.select('br')]
        for key, value in _getdict(lines, apply=int).items():
            yield Field(_out_name(key), value)
