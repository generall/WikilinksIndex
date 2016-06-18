
require 'elasticsearch'

class WikiSeracher

  def initialize
    @client = Elasticsearch::Client.new log: false
  end

  def add_mention(mention)
    @client.index index: 'wiki', type: 'mention', body: mention
  end



  def add_mentions(mentions)
    @client.bulk body: mentions.map{ |mention|
      {
        index: {
          _index: 'wiki',
          _type: 'mention',
          data: mention
        }
      }
    }
  end
end
