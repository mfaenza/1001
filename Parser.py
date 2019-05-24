import ftfy
import pymongo
from urllib.request import Request, urlopen

class Parser:
    
    def __init__(self):
        
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        db = myclient['1001']
        
        self.url_html_map = db['url_html_map']
        self.tracklist_collection = db['tracklist_docs']
        self.played_collection = db['played_docs']
        self.track_collection = db['track_docs']
        self.artist_collection = db['artist_docs']
        self.sequential_collection = db['sequential_docs']
        
        self.track_docs = []
        self.played_docs = []
        self.artist_docs = []
        self.tracklist_docs = []
        self.sequential_docs = []

    def request(self, url):

        req = Request(url,\
                      headers={'User-Agent': 'Mozilla/5.0'})
        html = str(urlopen(req).read())
        return html
        
    def find_str(self, s, char, start_index=0):
        '''
        Find substring char in string s. Found on internet, probably not efficient.

        '''
        index = 0
        s = s[start_index+1:]
        if char in s:
            c = char[0]
            for ch in s:
                if ch == c:
                    if s[index:index+len(char)] == char:
                        return start_index + 1 + index
                index += 1
        return -1

    def fix_decoding_errors(self, string):
        '''
        Fix UTF-8 decoding issues. Probably need to find more systematic/thorough approach to this.

        REPLACE THIS WITH ftfy.fix_text() -- python package which should be one stop shop for fixes
        '''
        string = string.replace('&amp;','&')
        string = string.replace('&#39;',"'")
        string = string.replace('\\xc3\\xb6','o')
        string = string.replace('\\xc3\\xab','e')
        string = string.replace('\\xc3\\x9','u')
        string = string.replace('\\xc3\\xb8','o')
        string = string.replace("\\'","'")

        return ftfy.fix_text(string)

    def parse_track_and_artist(self, track_string):
        '''
        Extract the artist, track name, and remixer (if any) from the standard formatting used by 1001.

        '''
        # Check if Remix/Bootleg/Edit and parse accordingly
        if ('Remix' in track_string) or ('Bootleg' in track_string) or ('Edit' in track_string):

            artist, track_remixer = [string.strip(' ') for string in track_string.split(' - ')]
            track_remixer = [string.strip(' ') for string in track_remixer.split('(')]

            if len(track_remixer) > 2:
                track = track_remixer[0]
                remixer = '('.join(track_remixer[1:])
            else:
                track, remixer = track_remixer
                remixer = remixer.rstrip('Remix)').strip(' ')

        # If not remix, then should follow standard layout "Artist Name - Track Name"
        # This layout is expressed explicitly in html
        else:

            artist, track = [string.strip(' ') for string in track_string.split(' - ')]
            remixer = 'N/A'

        # Check for multiple artists -- Big Room sets tend to have hella mashups
        # Sometimes there is more structured formatting to exploit i.e. (Artist1 vs. Artist2 - Track1 vs. Track2)
        # Not worrying about that now b/c big room sux
        if 'vs.' in artist:
            artist = artist.replace('vs.','&')
        if '&' in artist:
            artist = [a.strip(' ') for a in artist.split('&')]

        # Remove features
        # We could make features a separate field but for now just removing
        if isinstance(artist, str):
            if ('feat.' in artist) or ('ft.' in artist):
                artist = artist.split('feat.')[0].strip(' ')
                artist = artist.split('ft.')[0].strip(' ')
        if isinstance(artist, list):
                artist = [a.split('feat.')[0].split('ft.')[0].strip(' ') for a in artist]

        if isinstance(artist, list):
            return (artist, track, remixer)
        else:
            return ([artist], track, remixer)


    def tracklist_meta_data(self, html):
        '''
        Extract meta data about tracklist/set.

        '''
        meta_data = {}

        # Extract set description
        index = 0
        start_term = 'meta name="description" content="'
        index = self.find_str(html, start_term, index)
        description = html[index:].split('>')[0]
        description = description.lstrip(start_term).rstrip('"')
        meta_data['description'] = description

        # Set creation date
        # Should probably be the point in time we use for building prediction data
        index = 0
        start_term = 'meta name="dcterms.created" content="'
        index = self.find_str(html, start_term, index)
        created = html[index:].split('>')[0]
        created = created.lstrip(start_term).rstrip('"')
        meta_data['created'] = created

        # Set last modified data
        index = 0
        start_term = 'meta name="dcterms.modified" content="'
        index = self.find_str(html, start_term, index)
        modified = html[index:].split('>')[0]
        modified = modified.lstrip(start_term).rstrip('"')
        meta_data['modified'] = modified

        return meta_data

    def tracklist_general_information(self, html):
        '''
        Extract general info about tracklist/set.

        '''
        info_doc = {}
        index = 0
        start_term = 'General Information'
        index = self.find_str(html, start_term, index)
        info_chunk = html[index:].split('Most Liked Tracklists')[0]

        # Genres -- can use these to build genre-specific graphs
        style_index = 0
        style_index = self.find_str(info_chunk, 'Tracklist Musicstyle', style_index)
        styles = info_chunk[style_index:].split('id="tl_music_styles">')[1].split('</td>')[0]
        styles = [style.strip(' ') for style in styles.split(',')]
        info_doc['styles'] = styles

        # If 1001 recognizes the dj who played the set they link their dj page
        # Its my understanding dj pages are independent of artist pages -- we'll need to map these
        index = 0
        start_term = 'a href="/dj'
        index = self.find_str(html, start_term, index)
        if index != -1:
            dj_url = html[index:].split('class')[0].split('"')[1]
            dj_url = 'https://www.1001tracklists.com' + dj_url
            info_doc['dj_url'] = dj_url

            dj_name = html[index:].split('</a>')[0].split('>')[1]
            info_doc['dj_name'] = dj_name
        else:
            info_doc['dj_url'] = 'N/A'
            info_doc['dj_name'] = 'N/A'

        return info_doc
        
    def tracklist_track_data(self, html):

        '''
        Extract track related data from set
        '''
        track_docs = {}
        index = 0
        while self.find_str(html, 'tracknumber_value">', index) != -1:

            index = self.find_str(html, 'tracknumber_value">', index)
            #print(index)
            track_chunk = html[index:].split('<br>')[0]

            # Extract track number
            track_num = track_chunk[:22].split('<')[0].strip('tracknumber_value">')
            #print('Track Number:', track_num)

            # Extract track information
            chunk_index = 0
            chunk_index = self.find_str(track_chunk, 'meta itemprop="name" content=', chunk_index)
            extracted_value = track_chunk[chunk_index:].strip('meta itemprop="name" content=').split('>')[0].strip('"')
            clean_string = self.fix_decoding_errors(extracted_value)
            #print(clean_string)

            if len(clean_string) > 1:
                try:
                    artist_list, track, remixer = self.parse_track_and_artist(clean_string)
                except:
                    artist_list, track, remixer = None, None, None
            else:
                artist_list, track, remixer = None, None, None
            #print(artist_list, track, remixer)
                
            # Avoid ID's for now
            if artist_list is None:
                pass
            # If track info pull failed then pass
            elif (('ID' in artist_list) or ('ID' in track)):
                pass
            else:

                # Tends to be multiple artists so artists parsed to list even if only one
                for artist in artist_list:

                    #print('Artist:',artist)
                    #print('Track:', track)
                    #print('Remixer:', remixer)

                    # Extract artist page
                    artist_index = 0
                    artist_index = self.find_str(track_chunk, 'title="open artist page"', artist_index)
                    if artist_index != -1:
                        try:
                            artist_url = track_chunk[artist_index:].split('class')[1].split('href="')[1].rstrip('" ')
                            artist_url = 'https://www.1001tracklists.com' + artist_url
                            #print('Aritst url:', artist_url)
                        except:
                            artist_url = 'N/A'
                    else:
                        artist_url = 'N/A'

                    # Extract remixer page (if exists)
                    if remixer != 'N/A':
                        remixer_index = self.find_str(track_chunk, 'title="open remixer artist page"', artist_index)
                        if remixer_index != -1:
                            try:
                                remixer_url = track_chunk[remixer_index:].split('class')[1].split('href="')[1].rstrip('" ')
                                remixer_url = 'https://www.1001tracklists.com' + remixer_url
                                #print('Remixer url:', remixer_url)
                            except:
                                remixer_url = 'N/A'
                        else:
                            remixer_url = 'N/A'
                    else:
                        remixer_url = 'N/A'
                    
                    # Extract track page
                    track_index = 0
                    track_index = self.find_str(track_chunk, 'title="open track page"', artist_index)
                    if track_index != -1:
                        try:
                            track_url = track_chunk[track_index:].split('class')[1].split('href="')[1].split('"')[0]
                            track_url = 'https://www.1001tracklists.com' + track_url
                            #print('track url:', track_url)
                        except:
                            track_url = 'N/A'
                    else:
                        track_url = 'N/A'

                    track_doc = {\
                                'track_num': track_num,
                                'artist': artist.strip(' '),
                                'artist_url': artist_url.strip(' '),
                                'name': track.strip(' '),
                                'track_url': track_url.strip(' '),
                                'remixer': remixer.strip(' '),
                                'remixer_url': remixer_url.strip(' ')
                                }
                    track_docs[track_num] = track_doc
                    #print('\n\n\n')
        
        #print(len(track_docs.keys()))
        return track_docs

    def build_artist_edges(self, url_doc, url):
        '''
        Build artist set-adjacency docs -- order n^2.
        Dont iterate over full set twice since will be considered non-directional
        '''
        all_tracks = {}
        count = 0
        these_tracks = list(url_doc['track_docs'].values())
        for i in range(len(these_tracks)):
            for j in range(i,len(these_tracks)):

                track = these_tracks[i]
                other_track = these_tracks[j]

                first_artist = track['artist']
                second_artist = other_track['artist']

                if first_artist != second_artist:
                    _id = '_'.join([url,first_artist,second_artist])
                    all_tracks[_id] = \
                                        {
                                        #'_id': _id,
                                        'artist1': first_artist,
                                        'artist2': second_artist,
                                        'url': url
                                        }
        return all_tracks

    def build_track_edges(self, track_docs, url):
        '''
        Build track set-adjacency docs -- order n^2.
        Dont iterate over full set twice since will be considered non-directional
        '''
        edge_docs = {}
        keys = sorted(list(track_docs.keys()))
        for i in range(len(keys)):
            for j in range(i, len(keys)):

                key = keys[i]
                other_key = keys[j]

                if key != other_key:
                    _id = '_'.join([url,'_'.join(key),'_'.join(other_key)])
                    edge_docs[_id] = \
                                        {
                                        #'_id': _id,
                                        'track1': key,
                                        'track2': other_key,
                                        'url': url
                                        }
        return edge_docs
                
    def build_sequential_track_edges(self, track_docs, url):
        '''
        Allows for later "next track lookup" functionality

        '''
        enumerated_tracks = [(track_docs[key]['track_num'], key) for key in list(track_docs.keys())]
        enumerated_tracks = sorted(enumerated_tracks, key=lambda x: x[0])

        seq_docs = {}
        for track_idx in range(len(enumerated_tracks)-1):
            _id = '_'.join(\
                          [\
                           url,\
                           '_'.join(enumerated_tracks[track_idx][1]),\
                           '_'.join(enumerated_tracks[track_idx+1][1])
                          ]
                        )
            seq_docs[_id] = \
                           {
                           #'_id': _id,
                           'url': url,
                           'first_track': enumerated_tracks[track_idx][1],
                           'second_track': enumerated_tracks[track_idx+1][1],
                           'first_position': str(enumerated_tracks[track_idx][0]),
                           'second_position': str(enumerated_tracks[track_idx+1][0]),   
                           }
        return seq_docs

    def build_played_playedby_edge(self, url_doc, url):
        '''
        Allows you to map who plays who.
        I think it would be interesting to study directional graphs from this.

        '''
        dj_name = url_doc['dj_name']
        dj_url = url_doc['dj_url']

        if (dj_name == 'N/A') or (dj_url == 'N/A'):
            return {}

        played_docs = {}
        for track_doc in list(url_doc['track_docs'].values()):
            _id = '_'.join([url,dj_name,track_doc['name']])
            played_docs[_id] = \
                              {
                              #'_id': _id,
                              'url': url,
                              'played_by': dj_name,
                              'played_by_url': dj_url,
                              'played': track_doc['name'],
                              'played_track_url': track_doc['track_url'],
                              'played_artist': track_doc['artist'],
                              'played_artist_url': track_doc['artist_url'],
                              'played_remixer': track_doc['remixer'],
                              'played_remixer_url': track_doc['remixer_url']
                              }
        return played_docs
    
    
    def parse(self, url, html): 

        url_doc = {}
        url_doc['url'] = url
        url_doc['html'] = html
        self.url_html_map.insert_one(url_doc)
            
        try:
            
            url_doc.update(self.tracklist_meta_data(html))
            url_doc.update(self.tracklist_general_information(html))

            track_docs = self.tracklist_track_data(html)
            url_doc['track_docs'] = track_docs

            track_edges = self.build_track_edges(track_docs, url).values()
            sequential_edges = self.build_sequential_track_edges(track_docs, url).values()
            played_edges = self.build_played_playedby_edge(url_doc, url).values()
            artist_edges = self.build_artist_edges(url_doc, url).values()

            self.tracklist_collection.insert_one(url_doc)
            for doc in track_edges:
                self.track_collection.insert_one(doc)
            for doc in artist_edges:
                self.artist_collection.insert_one(doc)
            for doc in played_edges:
                self.played_collection.insert_one(doc)
            for doc in sequential_edges:
                self.sequential_collection.insert_one(doc)

            self.played_docs.extend(played_edges)
            self.track_docs.extend(track_edges)
            self.tracklist_docs.append(url_doc)
            self.artist_docs.extend(artist_edges)        
            self.sequential_docs.extend(sequential_edges)

            print('Len played docs:', len(self.played_docs))
            print('Len sequential docs:', len(self.sequential_docs))
            print('Len track docs:', len(self.track_docs))
            print('Len tracklist docs:', len(self.tracklist_docs))
            print('Len artist docs:', len(self.artist_docs))
        
        except Exception as e:
            print(str(e))