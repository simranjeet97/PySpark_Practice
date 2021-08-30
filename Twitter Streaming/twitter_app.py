import tweepy
import json
from tweepy.streaming import StreamListener
class TweetListener(StreamListener):
  # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket
    def on_data(self, data):
        try:  
            # Load data
            msg = json.loads(data)
            # Read extended Tweet if available
            if "extended_tweet" in msg:
                # Add "__end" at the end of each Tweet 
                self.client_socket\
                    .send(str(msg['extended_tweet']['full_text']+" __end")\
                    .encode('utf-8'))         
                print(msg['extended_tweet']['full_text'])
            # Else read Tweet text
            else:
                # Add "__end" at the end of each Tweet
                self.client_socket\
                    .send(str(msg['text']+"__end")\
                    .encode('utf-8'))
                print(msg['text'])
            return True
        except BaseException as e:
            print("error on_data: %s" % str(e))
        return True
    def on_error(self, status):
        print(status)
        return True



API_KEY=''
API_SECRET=''
ACCESS_TOKEN =''
ACCESS_SECRET=''


from tweepy import Stream
from tweepy import OAuthHandler

def sendData(c_socket, keyword):
    print("Start sending data from Twitter to socket")
    # Authentication based on the developer credentials from twitter
    auth = OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    # Send data from the Stream API
    twitter_stream = Stream(auth, TweetListener(c_socket))
    # Filter by keyword and language
    twitter_stream.filter(track = keyword, languages=["en"])


import socket
if __name__ == "__main__":
    # Create listening socket on server (local)
    s = socket.socket()
    # Host address and port
    host = "127.0.0.1"
    port = 9009
    s.bind((host, port))
    print("Socket is established")
    # Server listens for connections
    s.listen(4)
    print("Socket is listening")
    # Return the socket and the address of the client
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # Send data to client via socket for selected keyword
    sendData(c_socket, keyword = ['covid'])
