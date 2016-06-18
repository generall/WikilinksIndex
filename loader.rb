require 'rubygems'
require 'bundler/setup'

require 'pry'
require_relative './gen-rb/wiki-link-v0.1_types.rb'
require_relative './search_client.rb'


DATA_FILE="/home/generall/data/dbpedia/wikilinks/002"


def convert_to_json(wikilink)
  wikilink.mentions.map do |mention|
    context = mention.context
    {
      source: wikilink.url,
      concept: mention.wiki_url,
      anchor_text: mention.anchor_text,
      context:
      {
        left: context ? context.left : "",
        middle: context ? context.middle : mention.anchor_text,
        right: context ? context.right : "",
      }
    }
  end



end

def main()

  ARGV.each do |file|

    transport = Thrift::BufferedTransport.new(File.new(file));
    protocol = Thrift::BinaryProtocol.new(transport)
    searcher = WikiSeracher.new

    item = WikiLinkItem.new
    urls = []
    buffer = []
    n = 0

    begin
      while item.read(protocol) == nil
        mentions = convert_to_json(item)
        mentions.each do |mention|
          urls.push mention[:concept]
          buffer.push mention
          if buffer.size > 10000
            searcher.add_mentions(buffer)
            buffer.clear
            n += 1
            puts n
          end
        end
      end
    rescue Exception => e
      puts e.message
    end

    File.write("urls.txt", urls.join('\n') + '\n', mode: 'a')
    urls.clear
  end

  binding.pry
end

if __FILE__ == $0
  main
end
