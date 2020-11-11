# CryptoKitties benchmark
KittyCore.bin: Main contract
    Constructor:
        - ceoAddress = msg.sender
        - cooAddress = msg.sender

setNewAddress(address _v2Address) onlyCEO whenPaused
    - newContractAddress = _v2Address

setSaleAuctionAddress(address _address) onlyCEO
    - saleAuction = SaleClockAuction(_address)

setSiringAuctionAddress(address _address) onlyCEO
    - siringAuction = SiringClockAuction(_address)

setGeneScienceAddress(address _address) onlyCEO
    - geneScience = GeneScienceInterface(_address)


setCEO(address _newCEO) onlyCEO
    ceoAddress = _newCEO;

setCFO(address _newCFO) onlyCEO
    cfoAddress = _newCFO;

setCOO(address _newCOO) onlyCEO
    cooAddress = _newCOO;