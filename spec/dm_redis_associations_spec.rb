require File.expand_path("../spec_helper", __FILE__)
require 'dm-core'
require 'dm-redis-adapter/spec/setup'
require 'dm-validations'
require 'dm-migrations'

describe DataMapper::Adapters::RedisAdapter do
  before(:all) do

    DataMapper.setup(:default, {
      :adapter  => "redis",
      :db => 15
    })

    DataMapper::Model.raise_on_save_failure = true

    redis = Redis.new(:db => 15)
    redis.flushdb

    class Book
      include DataMapper::Resource

      property :id,   Serial
      property :name, String, :index => true

      has n, :book_tags
      has n, :tags, :through => :book_tags
    end

    class Tag
      include DataMapper::Resource

      property :id,   Serial
      property :name, String

      has n, :book_tags
      has n, :books, :through => :book_tags
    end

    class BookTag
      include DataMapper::Resource

      property :id,   Serial

      belongs_to :book
      belongs_to :tag
    end

    DataMapper.finalize
    DataMapper.auto_migrate!

    @b = Book.create(:name => "Harry Potter")
    @t = Tag.create(:name => "fiction")
    @t2 = Tag.create(:name => "wizards")
    
    @b.tags << @t
    @b.tags << @t2
    @b.save
    

    @b2 = Book.create(:name => "A Series of Unfortunate Events")
    @t3 = Tag.create(:name => "olaf")

    @b2.tags << @t
    @b2.tags << @t3
    @b2.save
  end

  it "should allow has n :through" do
    #b2 = Book.get(1)
    #b2.tags.should == [@t,@t2]
    debugger
    b2 = Book.get(1)
    b2.tags.should == [@t,@t3]
  end

  it "should allow inclusion in operators for belongs_to" do
    BookTag.first(:book => [@b]).should be
  end

  it "should allow inclusion in operators" do
    Book.first(:name => ["Harry Potter"]).should == @b
  end

  it "should allow inclusion in operators for associations" do
    BookTag.first(:tag => [@t]).book.should == @b
    BookTag.all(:tag => [@t]).to_a.map {|x| x.book}.should == [@b,@b2]
  end
end
