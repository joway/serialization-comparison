package main

import (
	"os"

	"github.com/linkedin/goavro"
)

const schema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "Username", "type": "string"}]}`

func main() {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}

	m := map[string]interface{}{
		"Username": "superman",
	}

	// Let's dip our feet into just encoding a single item into binary format.
	// There is not much to do with the output from binary if you intend on
	// creating an OCF file, because OCF will do this encoding for us.  The
	// result is an unadorned stream of binary bytes that can never be decoded
	// unless you happen to know the schema that was used to encode it.
	binary, err := codec.BinaryFromNative(nil, m)
	if err != nil {
		panic(err)
	}
	_ = binary

	// Next, let's try encoding the same item using Single-Object Encoding,
	// another format that is useful when sending a bunch of objects into a
	// Kafka stream.  Note this method prefixes the binary bytes with a schema
	// fingerprint, used by the reader on the stream to lookup the contents of
	// the schema used to encode the value.  Again, unless the reader can fetch
	// the schema contents from a schema source-of-truth, this binary sequence
	// will never be decodable.
	single, err := codec.SingleFromNative(nil, m)
	if err != nil {
		panic(err)
	}
	_ = single

	// Next, let's make an OCF file from the values.  The OCF format prefixes
	// the entire file with the required schema that was used to encode the
	// data, so it is readable from any Avro decoder that can read OCF files.
	// No other source of information is needed to decode the file created by
	// this process, unlike the above two examples.  Also note that we do not
	// send OCF the encoded blobs to write, but just append the values and it
	// will encode each of the values for us.
	var values []map[string]interface{}
	values = append(values, m)
	values = append(values, map[string]interface{}{"Username": "batman"})
	values = append(values, map[string]interface{}{"Username": "wonder woman"})

	f, err := os.Create("event.avro")
	if err != nil {
		panic(err)
	}
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     f,
		Codec: codec,
	})
	if err != nil {
		panic(err)
	}
	if err = ocfw.Append(values); err != nil {
		panic(err)
	}
}
