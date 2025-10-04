defmodule Kayrock.CompressionTest do
  use ExUnit.Case

  alias Kayrock.Compression
  
  @test_data "The quick brown fox jumps over the lazy dog"
  @large_data String.duplicate("Hello World! ", 1000)

  # Simple macro to skip tests if codec not available
  defmacro if_available(codec, do: block) do
    quote do
      mod = case unquote(codec) do
        :gzip -> Kayrock.Compression.Gzip
        :snappy -> Kayrock.Compression.Snappy
        :lz4 -> Kayrock.Compression.Lz4
        :zstd -> Kayrock.Compression.Zstd
      end
      
      if mod.available?() do
        unquote(block)
      end
    end
  end

  describe "gzip" do
    test "compress/decompress" do
      {compressed, 1} = Compression.compress(:gzip, @test_data)
      assert Compression.decompress(1, compressed) == @test_data
    end

    test "compression levels" do
      {compressed_1, _} = Compression.compress(:gzip, @large_data, level: 1)
      {compressed_9, _} = Compression.compress(:gzip, @large_data, level: 9)
      assert byte_size(compressed_9) <= byte_size(compressed_1)
    end
  end

  describe "snappy" do
    test "compress/decompress" do
      if_available :snappy do
        {compressed, 2} = Compression.compress(:snappy, @test_data)
        assert Compression.decompress(2, compressed) == @test_data
      end
    end
  end

  describe "lz4" do
    test "compress/decompress" do
      if_available :lz4 do
        {compressed, 3} = Compression.compress(:lz4, @test_data)
        assert Compression.decompress(3, compressed) == @test_data
      end
    end
    
    test "error handling" do
      if_available :lz4 do
        # Just verify it doesn't crash on edge cases
        {compressed, _} = Compression.compress(:lz4, "")
        assert Compression.decompress(3, compressed) == ""
      end
    end
  end

  describe "zstd" do
    test "compress/decompress" do
      if_available :zstd do
        {compressed, 4} = Compression.compress(:zstd, @test_data)
        assert Compression.decompress(4, compressed) == @test_data
      end
    end

    test "compression levels" do
      if_available :zstd do
        {compressed_1, _} = Compression.compress(:zstd, @large_data, level: 1)
        {compressed_22, _} = Compression.compress(:zstd, @large_data, level: 22)
        assert byte_size(compressed_22) <= byte_size(compressed_1)
      end
    end
  end

  test "available_codecs includes at least gzip" do
    codecs = Compression.available_codecs()
    assert :gzip in codecs
  end

  test "all available codecs round-trip correctly" do
    for codec <- Compression.available_codecs() do
      {compressed, attr} = Compression.compress(codec, @test_data)
      assert Compression.decompress(attr, compressed) == @test_data
    end
  end

  test "snappy decompression works with chunked messages" do
    data =
      <<130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 14, 12, 44, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3,
        246, 0, 0, 0, 75, 246, 7, 92, 10, 44, 16, 236, 0, 0, 255, 255, 255, 255, 0, 0, 3, 232, 65, 66, 67, 68, 69, 70,
        71, 72, 73, 74, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0,
        254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 118, 10, 0>>

    expected =
      <<0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3, 246, 10, 44, 16, 236, 0, 0, 255, 255, 255, 255, 0, 0, 3, 232>> <>
        String.duplicate("ABCDEFGHIJ", 100)

    assert expected == Compression.decompress(2, data)
  end
end
