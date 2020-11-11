pragma solidity >0.4.24;
import "./scalable_coin_interface.sol";

contract Account is AccountInterface {
  uint public id;
  uint public ownedTokens;
  address public owner;
  STokenInterface public parent;
  mapping (address => uint) public allowed;

  modifier onlyOwner {
    require(owner == msg.sender, "different owner");
    _;
  }

  event Debit(uint id);

  // constructor(uint _id, uint _tokens, STokenInterface _parent, address _owner) public {
  constructor(uint _id, uint _tokens, STokenInterface _parent, address _owner) public {
    id = _id;
    parent = _parent;
    owner = _owner;
    ownedTokens = _tokens;
    emit CreatedAccount(address(this), id);
  }

  function moveTo(uint shardId) public {
    require(msg.sender == owner);
    assembly {
        move(shardId)
    }
  }

  function balance() public view returns (uint) {
    return ownedTokens;
  }

  function allowance(address _spender) public view returns (uint) {
    return allowed[_spender];
  }

  function transfer(AccountInterface _to, uint _tokens) public onlyOwner returns (bool) {
    require(_tokens <= ownedTokens);
    ownedTokens -= _tokens;
    _to.debit(_tokens, id);
    emit Transfer(_to, _tokens);
    return true;
  }

  function approve(address _spender, uint _tokens) public onlyOwner returns (bool) {
    allowed[_spender] = _tokens;
    emit Approval(_spender, _tokens);
    return true;
  }

  function transferFrom(AccountInterface _to, uint _tokens) public returns (bool) {
    require(_tokens <= ownedTokens);
    require(_tokens <= allowed[msg.sender]);
    allowed[msg.sender] -= _tokens;
    ownedTokens -= _tokens;
    _to.debit(_tokens, id);
    emit Transfer(_to, _tokens);
    return true;
  }

  function debit(uint _tokens, uint _id) public returns (bool) {
    Account caller = Account(msg.sender);
    require(parent == caller.parent(), "Not the same parent");
    bytes memory bytePack = abi.encodePacked(byte(0xff), parent, caller.id());
    address addr = address(uint(keccak256(bytePack)));
    require(msg.sender == addr, "Wrong contract");
    ownedTokens += _tokens;
    return true;
  }
}

contract ScalableCoin is STokenInterface {
  string public constant name = "Scalable Coin";
  string public constant symbol = "SCO";
  uint8 public constant decimals = 18;
  uint constant supply = 1e19;
  uint tokensSold;
  uint id = 0;

  function toBytes(uint _tokens, STokenInterface _parent, address _owner) internal view returns (bytes memory) {
    return abi.encode(id, _tokens, _parent, _owner);
  }

  function concat(
        bytes memory _preBytes,
        bytes memory _postBytes
    )
        internal
        pure
        returns (bytes memory)
    {
        bytes memory tempBytes;

        assembly {
            // Get a location of some free memory and store it in tempBytes as
            // Solidity does for memory variables.
            tempBytes := mload(0x40)

            // Store the length of the first bytes array at the beginning of
            // the memory for tempBytes.
            let length := mload(_preBytes)
            mstore(tempBytes, length)

            // Maintain a memory counter for the current write location in the
            // temp bytes array by adding the 32 bytes for the array length to
            // the starting location.
            let mc := add(tempBytes, 0x20)
            // Stop copying when the memory counter reaches the length of the
            // first bytes array.
            let end := add(mc, length)

            for {
                // Initialize a copy counter to the start of the _preBytes data,
                // 32 bytes into its memory.
                let cc := add(_preBytes, 0x20)
            } lt(mc, end) {
                // Increase both counters by 32 bytes each iteration.
                mc := add(mc, 0x20)
                cc := add(cc, 0x20)
            } {
                // Write the _preBytes data into the tempBytes memory 32 bytes
                // at a time.
                mstore(mc, mload(cc))
            }

            // Add the length of _postBytes to the current length of tempBytes
            // and store it as the new length in the first 32 bytes of the
            // tempBytes memory.
            length := mload(_postBytes)
            mstore(tempBytes, add(length, mload(tempBytes)))

            // Move the memory counter back from a multiple of 0x20 to the
            // actual end of the _preBytes data.
            mc := end
            // Stop copying when the memory counter reaches the new combined
            // length of the arrays.
            end := add(mc, length)

            for {
                let cc := add(_postBytes, 0x20)
            } lt(mc, end) {
                mc := add(mc, 0x20)
                cc := add(cc, 0x20)
            } {
                mstore(mc, mload(cc))
            }

            // Update the free-memory pointer by padding our last write location
            // to 32 bytes: add 31 bytes to the end of tempBytes to move to the
            // next 32 byte block, then round down to the nearest multiple of
            // 32. If the sum of the length of the two arrays is zero then add 
            // one before rounding down to leave a blank 32 bytes (the length block with 0).
            mstore(0x40, and(
              add(add(end, iszero(add(length, mload(_preBytes)))), 31),
              not(31) // Round down to the nearest 32 bytes.
            ))
        }

        return tempBytes;
    }


  function totalSupply() public view returns (uint) {
    return supply;
  }

  function newAccountFor(address _forAddr) public payable returns (AccountInterface, uint) {
    ++id;
    uint ownedTokens = msg.value;
    tokensSold += ownedTokens;
    assert(tokensSold <= supply);

    // address account = address(new Account(id, this, _forAddr, ownedTokens));
    address account;
    // bytes memory _code = type(Account).creationCode;
    bytes memory _code = concat(type(Account).creationCode, toBytes(ownedTokens, this, _forAddr));
    assembly {
      account := create2(0, add(_code, 0x20), mload(_code), sload(id_slot))
      if iszero(extcodesize(account)) {revert(0, 0)}
    }
    emit CreatedAccount(account, id);
    return (AccountInterface(account), id);
  }

  function newAccount() public payable returns (AccountInterface, uint) {
    return newAccountFor(msg.sender); 
  }
}
