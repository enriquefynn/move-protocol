pragma solidity >0.4.25;

import './GeneScience.sol';

contract SERC721 {
    function approve(address _to) external;
    function transfer(address _to) external;
    function transferFrom(address _to) external;
    function moveTo(uint256 _toShard) external;

    // Events
    event Transfer(address from, address to, Kitty tokenId);
    event Approval(address owner, address approved, Kitty tokenId);

    // Optional
    // function name() public view returns (string name);
    // function symbol() public view returns (string symbol);
    // function tokensOfOwner(address _owner) external view returns (uint256[] tokenIds);
    // function tokenMetadata(uint256 _tokenId, string _preferredTransport) public view returns (string infoUrl);

    // function totalSupply() public view returns (uint256 total);
    // function balanceOf(address _owner) public view returns (uint256 balance);
    // function ownerOf(uint256 _tokenId) public view returns (address owner);

}

contract Breeder {
    uint32 lastId;
    GeneScience geneScience;
    constructor(GeneScience _geneScience) public {
        geneScience = _geneScience;
    }

    function _isValidMatingPair(
        Kitty _matron,
        Kitty _sire
    )
        private
        view
        returns(bool)
    {
        // A Kitty can't breed with itself!
        if (_matron.kittyId() == _sire.kittyId()) {
            return false;
        }

        // Kitties can't breed with their parents.
        if (_matron.matronId() == _sire.kittyId() || _matron.sireId() == _sire.kittyId()) {
            return false;
        }
        if (_sire.matronId() == _matron.kittyId() || _sire.sireId() == _matron.kittyId()) {
            return false;
        }

        // We can short circuit the sibling check (below) if either cat is
        // gen zero (has a matron ID of zero).
        if (_sire.matronId() == 0 || _matron.matronId() == 0) {
            return true;
        }

        // Kitties can't breed with full or half siblings.
        if (_sire.matronId() == _matron.matronId() || _sire.matronId() == _matron.sireId()) {
            return false;
        }
        if (_sire.sireId() == _matron.matronId() || _sire.sireId() == _matron.sireId()) {
            return false;
        }

        // Everything seems cool! Let's get DTF.
        return true;
    }

    function breed(Kitty matron, Kitty sire)
        external
        payable
    {
        // TODO: Check both matron and sire are valid

        address matronOwner = matron.owner();
        // Caller must own the matron.
        require(msg.sender == matronOwner);

        // Neither sire nor matron are allowed to be on auction during a normal
        // breeding operation, but we don't need to check that explicitly.
        // For matron: The caller of this function can't be the owner of the matron
        //   because the owner of a Kitty on auction is the auction house, and the
        //   auction house will never call breedWith().
        // For sire: Similarly, a sire on auction will be owned by the auction house
        //   and the act of transferring ownership will have cleared any oustanding
        //   siring approval.
        // Thus we don't need to spend gas explicitly checking to see if either cat
        // is on auction.

        // Check that matron and sire are both owned by caller, or that the sire
        // has given siring permission to caller (i.e. matron's owner).
        // Will fail for _sireId = 0
        require(matronOwner == sire.owner() || sire.sireAllowedToAddress() == matronOwner);

        // Make sure matron isn't pregnant, or in the middle of a siring cooldown
        require(matron.siringWithId() == 0);

        // Make sure sire isn't pregnant, or in the middle of a siring cooldown
        require(sire.siringWithId() == 0);

        // Test that these cats are a valid mating pair.
        // require(_isValidMatingPair(matron, sire));

        // All checks passed, kitty gets pregnant!
        matron.matronBreedWith(sire);
        sire.sireBreedWith();
    }

    function _createKitty(
        uint256 _matronId,
        uint256 _sireId,
        uint256 _generation,
        uint256 _genes,
        address _owner
    )
       internal 
       returns (Kitty)
    {
        // These requires are not strictly necessary, our calling code should make
        // sure that these conditions are never broken. However! _createKitty() is already
        // an expensive call (for storage), and it doesn't hurt to be especially careful
        // to ensure our data structures are always valid.
        require(_matronId == uint256(uint32(_matronId)));
        require(_sireId == uint256(uint32(_sireId)));
        require(_generation == uint256(uint16(_generation)));

        // // New kitty starts with the same cooldown as parent gen/2
        // uint16 cooldownIndex = uint16(_generation / 2);
        // if (cooldownIndex > 13) {
        //     cooldownIndex = 13;
        // }

        Kitty kitten = new Kitty(lastId, _matronId, _sireId, _generation, _genes, _owner);

        // It's probably never going to happen, 4 billion cats is A LOT, but
        // let's just be 100% sure we never let this happen.
        require(lastId == uint256(uint32(lastId)));
        lastId++;

        return kitten;
    }

    // TODO: Make private
    function createPromoKitty(uint256 _genes, address _owner) external {
        _createKitty(0, 0, 0, _genes, _owner);
    }

    function giveBirth(Kitty matron) external returns(Kitty) {
        // TODO: Verify that matron was created by me

        // Check that the matron is pregnant, and that its time has come!
        require(matron.siringWithId() != 0);

        uint16 parentGen = matron.generation();
        if (matron.siringGeneration() > matron.generation()) {
            parentGen = matron.siringGeneration();
        }
        uint256 childGenes = 1;

        Kitty kitten = _createKitty(matron.kittyId(), matron.siringWithId(), parentGen + 1, childGenes, matron.owner());
        matron.completeBirth();

        // return the new kitten's address
        return kitten;
    }
}

contract Kitty is SERC721 {
    uint32 public kittyId;
    address public owner;
    uint256 public genes;
    uint64 public birthTime;
    uint64 public cooldownEndBlock;
    uint32 public matronId;
    uint32 public sireId;
    uint32 public siringWithId;
    uint16 public siringGeneration;
    uint16 public cooldownIndex;
    uint16 public generation;

    address public sireAllowedToAddress;
    address public kittyToApproved;

    address public parentContract;

    modifier onlyOwner() {
        require(msg.sender == owner);
        _;
    }
    event Birth(address kittyAddress, address owner, uint32 kittyId, uint32 matronId, uint32 sireId, uint256 genes);
    event Pregnant(address owner, uint256 matronId, uint256 sireId, uint256 cooldownEndBlock);

    function moveTo(uint _toShard) external {
        assembly {
            move(_toShard)
        } 
    }

    constructor(
        uint256 _kittyId,
        uint256 _matronId,
        uint256 _sireId,
        uint256 _generation,
        uint256 _genes,
        address _owner
    ) public {
        // These requires are not strictly necessary, our calling code should make
        // sure that these conditions are never broken. However! _createKitty() is already
        // an expensive call (for storage), and it doesn't hurt to be especially careful
        // to ensure our data structures are always valid.
        require(_generation == uint256(uint16(_generation)));

        // New kitty starts with the same cooldown as parent gen/2
        uint16 _cooldownIndex = uint16(_generation / 2);
        if (cooldownIndex > 13) {
            cooldownIndex = 13;
        }

        birthTime = uint64(now);

        cooldownEndBlock = 0;
        kittyId = uint32(_kittyId);
        matronId = uint32(_matronId);
        sireId = uint32(_sireId);
        genes = _genes;
        owner = _owner;
        cooldownIndex = _cooldownIndex;
        generation = uint16(_generation);

        // It's probably never going to happen, 4 billion cats is A LOT, but
        // let's just be 100% sure we never let this happen.

        // emit the birth event
        emit Birth(
            address(this),
            owner,
            kittyId,
            matronId,
            sireId,
            genes
        );

        // This will assign ownership, and also emit the Transfer event as
        // per ERC721 draft
        _transfer(address(0), _owner);
    }

    function completeBirth() public {
        // TODO: Breeder should be the only one to call this 

        delete siringWithId;
        delete siringGeneration;
    }
    
    function matronBreedWith(Kitty sire) public {
        // TODO: Breeder should be the only one to call this

        // Mark the matron as pregnant, keeping track of who the sire is.
        siringWithId = uint32(sire.kittyId());
        siringGeneration = uint16(sire.generation());

        // Trigger the cooldown for both parents.
        // _triggerCooldown(sire);
        // _triggerCooldown(matron);

        // Clear siring permission for both parents. This may not be strictly necessary
        // but it's likely to avoid confusion!
        delete sireAllowedToAddress;
        // delete sireAllowedToAddress[_sireId];

        // Every time a kitty gets pregnant, counter is incremented.
        // pregnantKitties++;

        // Emit the pregnancy event.
        emit Pregnant(owner, matronId, sire.kittyId(), cooldownEndBlock);
    }

    function sireBreedWith() public {
        // TODO: Breeder should be the only one to call this
        delete sireAllowedToAddress;
    }

    function transfer(address _to) external onlyOwner {
        // Safety check to prevent against an unexpected 0x0 default.
        require(_to != address(0));
        // Disallow transfers to this contract to prevent accidental misuse.
        // The contract should never own any kitties (except very briefly
        // after a gen0 cat is created and before it goes on auction).
        require(_to != address(this));

        // Reassign ownership, clear pending approvals, emit Transfer event.
        _transfer(msg.sender, _to);
    }

    function _transfer(address _from, address _to) internal {
        owner = _to;

        // Since the number of kittens is capped to 2^32 we can't overflow this
        // ownershipTokenCount[_to]++;
        // transfer ownership
        // kittyIndexToOwner[_tokenId] = _to;

        // When creating new kittens _from is 0x0, but we can't account that address.
        if (_from != address(0)) {
            // ownershipTokenCount[_from]--;
            // once the kitten is transferred also clear sire allowances
            delete sireAllowedToAddress;
            // clear any previously approved ownership exchange
            delete kittyToApproved;
        }
        // Emit the transfer event.
        emit Transfer(_from, _to, this);
    }

    function transferFrom(address _to) external {
        // Safety check to prevent against an unexpected 0x0 default.
        require(_to != address(0));
        // Disallow transfers to this contract to prevent accidental misuse.
        // The contract should never own any kitties (except very briefly
        // after a gen0 cat is created and before it goes on auction).
        require(_to != address(this));
        // Check for approval and valid ownership
        require(kittyToApproved == msg.sender);

        // Reassign ownership (also clears pending approvals and emits Transfer event).
        _transfer(address(this), _to);
    }

    function approve(address _to) external onlyOwner {

        // Register the approval (replacing any previous approval).
        kittyToApproved = _to;

        // Emit approval event.
        emit Approval(msg.sender, _to, this);
    }

    function approveSiring(address _addr) external onlyOwner {
        sireAllowedToAddress = _addr;
    }
} 