
require 'digest'


module EventBuf
  
  class Appender
  
    def initialize(dir, opt = {})
      @rotationSize = opt[:rotationSize].nil? ? 100000 : opt[:rotationSize]
      `mkdir -p '#{dir}'`           if opt[:mkdirp]
      raise "Dir does not exist"    if not (File.exists?(dir) and File.directory?(dir))
      @dir = dir
      @last_datecode = ''
      @num_with_last_datecode = 0
      _openNewFile()
    end
    
    def _openNewFile()
      t = Time.now.utc
      datecode = t.strftime "%Y-%m-%d-%H-%M-%S-#{timeL(t)}"
      if datecode == @last_datecode
        @num_with_last_datecode += 1
      else
        @last_datecode = datecode
        @num_with_last_datecode = 1
      end
      # ASSUMPTION: fewer than 10k file rotations per millisecond
      filename = t.strftime "events-#{datecode}-#{@num_with_last_datecode.to_s.rjust(4, "0")}.v1"
      # TODO: assert that filename is greater than existing filenames
      path = File.join @dir, filename
      @f = File.open path, 'ab'
      @f_size = 0
    end
    
    def log(data, opt = {})
      t = Time.now.utc
      h = Digest::SHA2.new
      
      sizeData = [data.length].pack "V"
      h.update sizeData
      @f.write sizeData
      
      tData = uint64le_pack((t.to_f * 1000).floor.to_i)
      h.update tData
      @f.write tData
      
      h.update data
      @f.write data
      
      @f.write h.digest().slice(0, 4)
      
      @f.flush
      if opt[:fsync].nil? or opt[:fsync]
        @f.fsync
      end
      
      @f_size += 12 + data.length
      if @f_size > @rotationSize
        @f.close
        _openNewFile()
      end
    end
  end
  
  
  class Reader
    
    def initialize(dir)
      @dir = dir
      @curFilename = ""
    end
    
    def peek()
      
      if not @currentEvent.nil?
        return @currentEvent
      else
        
        # Wait for dir to exist
        while not (File.exists?(@dir) and File.directory?(@dir))
          sleep 0.1
        end
        
        # Open file, if needed
        if @f.nil?
          while not _openNextFileIfExists()
            sleep 0.1
          end
        end
        
        while true
          # Are there more events in the current file?
          pos = @f.tell()# Save position
          begin
            
            # read/parse
            sizeData = @f.sysread 4
            size = sizeData.unpack("V")[0]
            afterSizeData = @f.sysread(8 + size + 4)
            msData = afterSizeData.slice(0, 8)
            ms = uint64le_unpack msData
            data = afterSizeData.slice(8, size)
            hash = afterSizeData.slice(8 + size, 4)
            
            # verify hash
            h = Digest::SHA2.new
            h.update sizeData
            h.update msData
            h.update data
            if h.digest().slice(0, 4) != hash
              raise "Invalid hash!"
            end
            
            @currentEvent = {
              :data => data,
              :ms => ms
            }
            return @currentEvent
            
          rescue EOFError
            if @f.tell() != pos
              @f.sysseek pos
            end
          end
          
          # If not, is there a new file?
          _openNextFileIfExists()
          
          sleep 0.1
        end
      end
    end
    
    def advance()
      @currentEvent = nil
    end
    
    def _nextFilename()
      Dir.foreach(@dir) do |filename|
        if filename.slice(0, 7) == "events-"
          if filename > @curFilename
            return filename
          end
        end
      end
      return nil
    end
    
    def _openNextFileIfExists()
      filename = _nextFilename()
      if filename.nil?
        return false
      else
        if not @f.nil?
          @f.close()
        end
        path = File.join @dir, filename
        @f = File.open path, 'rb'
        @curFilename = filename
        return true
      end
    end
    
  end
  
end



def sha256_first4(data)
  Digest::SHA2.digest(data).slice(0, 4)
end

def uint64le_pack(n)
  # WTF, no uint64le {,un}packing? Ruby don't care about data-encoding people!
  [n % 2**32, n >> 32].pack "VV"
end

def uint64le_unpack(data)
  o1, o2 = data.unpack 'VV'
  (o1 + (o2 << 32))
end

def timeL(t)
  ms = ((t.to_f * 1000) % 1000).floor
  ms.to_s.rjust(3, '0')
end

