% Copyright 2011,  Filipe David Manana  <fdmanana@apache.org>
% Web site:  http://github.com/fdmanana/couchfoo
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_file).

-export([open/1, close/1]).
-export([file_size/1, block_count/1, block_to_offset/1, offset_to_block/1]).
-export([header_count/3, find_header/3]).
-export([pread_iolist/2, pread_term/2]).
-export([open_header/2]).
-export([append_header/2]).

-include("couchfoo.hrl").

-define(SIZE_BLOCK, 4096).

-record(file, {
    fd,
    filename
}).


open(DbFile) ->
    case file:open(DbFile, [raw, read, binary]) of
    {ok, Fd} ->
        {ok, #file{fd = Fd, filename = DbFile}};
    {error, _} = Error ->
        Error
    end.


close(#file{fd = Fd}) ->
    ok = file:close(Fd).


block_to_offset(Block) ->
    Block * ?SIZE_BLOCK.


offset_to_block(Offset) ->
    Offset div ?SIZE_BLOCK.


file_size(#file{filename = FileName}) ->
    filelib:file_size(FileName).


block_count(File) ->
    Size = file_size(File),
    case Size rem ?SIZE_BLOCK of
    0 ->
        Size div ?SIZE_BLOCK;
    _ ->
        Size div ?SIZE_BLOCK + 1
    end.


header_count(File, StartOffset, EndOffset) ->
    {ValidCount, CorruptedCount} =
        header_count_loop(File, StartOffset, EndOffset, 0, 0),
    {ok, ValidCount, CorruptedCount}.


header_count_loop(_Fd, StartOffset, EndOffset, Valid, Corrupted)
        when StartOffset < EndOffset ->
    {Valid, Corrupted};

header_count_loop(Fd, StartOffset, EndOffset, Valid, Corrupted) ->
    case find_header(Fd, StartOffset, EndOffset) of
    {ok, _HeaderTuple, _HeaderBin, BlockFound} ->
        header_count_loop(
            Fd, block_to_offset(BlockFound - 1), EndOffset, Valid + 1, Corrupted);
    {corrupted_header, _Reason, BlockFound} ->
        header_count_loop(
            Fd, block_to_offset(BlockFound - 1), EndOffset, Valid, Corrupted + 1);
    _ ->
        NextStartOffset = block_to_offset(offset_to_block(StartOffset) - 1),
        header_count_loop(Fd, NextStartOffset, EndOffset, Valid, Corrupted)
    end.


open_header(#file{fd = Fd}, Offset) ->
    case (catch load_header(Fd, Offset)) of
    {ok, HeaderBin} ->
        couch_db_header:headerbin_to_tuple(HeaderBin);
    {corrupted_header, Reason} ->
        {corrupted_header, Reason};
    _ ->
        invalid_header
    end.


append_header(#file{filename = FileName} = File, Header) ->
    case file:open(FileName, [raw, read, binary, append]) of
    {ok, Fd} ->
        Bin = term_to_binary(Header),
        Md5 = erlang:md5(Bin),
        HeaderBin = <<Md5/binary, Bin/binary>>,
        Padding = case file_size(File) rem ?SIZE_BLOCK of
        0 ->
            <<>>;
        BlockOffset ->
            <<0:(8 * (?SIZE_BLOCK - BlockOffset))>>
        end,
        HeaderBinSize = byte_size(HeaderBin),
        FinalBin = [Padding, <<1, HeaderBinSize:32/integer>> | make_blocks(5, [HeaderBin])],
        Result = file:write(Fd, FinalBin),
        ok = file:close(Fd),
        Result;
    Error ->
        Error
    end.


%% All the following functions were copied, or are slight variations, from
%% Apache CouchDB's couch_file.erl

find_header(File, StartOffset, EndOffset) ->
    find_header_int(File, offset_to_block(StartOffset), EndOffset).

find_header_int(#file{fd = Fd} = File, Block, EndOffset) ->
    case block_to_offset(Block) < EndOffset of
    true ->
        no_valid_header;
    false ->
        case (catch load_header(Fd, Block * ?SIZE_BLOCK)) of
        {ok, HeaderBin} ->
            case couch_db_header:headerbin_to_tuple(HeaderBin) of
            {ok, HeaderTuple} ->
                {ok, HeaderTuple, HeaderBin, Block};
            {corrupted_header, Reason} ->
                {corrupted_header, Reason, Block}
            end;
        {corrupted_header, Reason} ->
            {corrupted_header, Reason, Block};
        _Error ->
            find_header_int(File, Block - 1, EndOffset)
        end
    end.


pread_term(File, Pos) ->
    case pread_iolist(File, Pos) of
    {ok, IoList} ->
        try
            {ok, binary_to_term(iolist_to_binary(IoList))}
        catch T:E ->
            {error, {T, E}}
        end;
    Error ->
        Error
    end.


pread_iolist(#file{fd = Fd}, Pos) ->
    catch pread_iolist_int(Fd, Pos).

pread_iolist_int(Fd, Pos) ->
    {LenIolist, NextPos} = read_raw_iolist_int(Fd, Pos, 4),
    case iolist_to_binary(LenIolist) of
    <<1:1/integer, Len:31/integer>> ->
        {Md5AndIoList, _} = read_raw_iolist_int(Fd, NextPos, Len + 16),
        {Md5, IoList} = extract_md5(Md5AndIoList),
        case erlang:md5(IoList) of
        Md5 ->
            {ok, IoList};
        _ ->
            {error, md5_check_failed}
        end;
    <<0:1/integer, Len:31/integer>> ->
        {Iolist, _} = read_raw_iolist_int(Fd, NextPos, Len),
        {ok, Iolist}
    end.


load_header(Fd, Offset) ->
    {ok, <<1, HeaderLen:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Offset, ?SIZE_BLOCK),
    TotalBytes = calculate_total_read_len(1, HeaderLen),
    case TotalBytes > byte_size(RestBlock) of
    false ->
        <<RawBin:TotalBytes/binary, _/binary>> = RestBlock;
    true ->
        {ok, Missing} = file:pread(
            Fd, Offset + 5 + byte_size(RestBlock),
            TotalBytes - byte_size(RestBlock)),
        RawBin = <<RestBlock/binary, Missing/binary>>
    end,
    <<Md5Sig:16/binary, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(1, RawBin)),
    case erlang:md5(HeaderBin) of
    Md5Sig ->
        {ok, HeaderBin};
    _ ->
        {corrupted_header, <<"Header MD5 checksum mismatch.">>}
    end.


calculate_total_read_len(0, FinalLen) ->
    calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
        FinalLen;
    BlockLeft ->
        FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK - 1)) +
            if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK - 1)) =:= 0 -> 0;
                true -> 1 end
    end.


remove_block_prefixes(_BlockOffset, <<>>) ->
    [];
remove_block_prefixes(0, <<_BlockPrefix, Rest/binary>>) ->
    remove_block_prefixes(1, Rest);
remove_block_prefixes(BlockOffset, Bin) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case byte_size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary, Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(0, Rest)];
    _Size ->
        [Bin]
    end.


read_raw_iolist_int(Fd, {Pos, _Size}, Len) ->
    read_raw_iolist_int(Fd, Pos, Len);
read_raw_iolist_int(Fd, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    {ok, <<RawBin:TotalBytes/binary>>} = file:pread(Fd, Pos, TotalBytes),
    {remove_block_prefixes(BlockOffset, RawBin), Pos + TotalBytes}.


extract_md5(FullIoList) ->
    {Md5List, IoList} = split_iolist(FullIoList, 16, []),
    {iolist_to_binary(Md5List), IoList}.


split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) when SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary, End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


make_blocks(_BlockOffset, []) ->
    [];
make_blocks(0, IoList) ->
    [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
    case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
    {Begin, End} ->
        [Begin | make_blocks(0, End)];
    _SplitRemaining ->
        IoList
    end.
