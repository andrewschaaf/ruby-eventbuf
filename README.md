
## Usage

* At any time at most one Appender and at most one Reader may be running for a specific dir.

### Appending
<pre>
require 'eventbuf'

appender = EventBuf::Appender.new "...some dir...", :mkdirp => true

# about 0.5 seconds
(1...1000).each do |i|
  appender.log "Foo"
end

# much faster
(1...1000).each do |i|
  appender.log "Foo", :fsync => false
end
</pre>


### Reading
<pre>
require 'eventbuf'

reader = EventBuf::Reader.new "...some dir..."
while true
  event = reader.peek()# blocks until there is one
  puts event[:ms]
  if upload_to_server event[:data]
    reader.advance()# NOT YET IMPLEMENTED
  end
end
</pre>

## File Format

See [node-eventbuf's README](https://github.com/andrewschaaf/node-eventbuf)
