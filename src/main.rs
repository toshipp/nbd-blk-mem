extern crate libc;
use std::os::unix::net::UnixStream;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::io::{Result, Error, Read, Write};
use std::fs::File;
use std::vec::Vec;
use std::mem::{uninitialized, transmute, size_of};
use std::slice;
use std::thread;

const NBD_SET_SOCK: u64 = 43776;
const NBD_SET_BLKSIZE: u64 = 43777;
const NBD_SET_SIZE: u64 = 43778;
const NBD_DO_IT: u64 = 43779;
const NBD_CLEAR_SOCK: u64 = 43780;
const NBD_CLEAR_QUE: u64 = 43781;
const NBD_PRINT_DEBUG: u64 = 43782;
const NBD_SET_SIZE_BLOCKS: u64 = 43783;
const NBD_DISCONNECT: u64 = 43784;
const NBD_SET_TIMEOUT: u64 = 43785;
const NBD_SET_FLAGS: u64 = 43786;

const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: [u8; 4] = [0x67, 0x44, 0x66, 0x98];

const NBD_CMD_READ: u32 = 0;
const NBD_CMD_WRITE: u32 = 1;
const NBD_CMD_DISC: u32 = 2;
const NBD_CMD_FLUSH: u32 = 3;
const NBD_CMD_TRIM: u32 = 4;

const BLOCK_SIZE: u64 = 4096;
const DEVICE_SIZE: u64 = 3 * 1024 * 1024 * 1024 / 2;

fn socketpair() -> Result<(UnixStream, UnixStream)> {
    let mut socks = [0 as libc::c_int; 2];
    unsafe {
        if libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, socks.as_mut_ptr()) != 0 {
            return Err(Error::last_os_error());
        }
        let client_sock = UnixStream::from_raw_fd(socks[0]);
        let server_sock = UnixStream::from_raw_fd(socks[1]);
        Ok((client_sock, server_sock))
    }
}

struct NBD {
    _nbd: File,
    _client_sock: UnixStream,
    server_sock: UnixStream,
    mem: Vec<u8>,
    th: thread::JoinHandle<libc::c_int>,
}

#[derive(Debug)]
#[repr(C)]
#[repr(packed)]
struct nbd_request {
    magic: u32,
    type_: u32,
    handle: [u8; 8],
    from: u64,
    len: u32,
}

/*
struct nbd_reply {
    magic: u32,
    error: u32,
    handle: [u8; 8],
}
*/

impl NBD {
    fn init() -> Result<NBD> {
        let f = File::open("/dev/nbd0")?;
        unsafe {
            let fd = f.as_raw_fd();
            if libc::ioctl(fd, NBD_CLEAR_SOCK) == -1 {
                return Err(Error::last_os_error());
            }
            if libc::ioctl(fd, NBD_SET_BLKSIZE, BLOCK_SIZE) == -1 ||
               libc::ioctl(fd, NBD_SET_SIZE_BLOCKS, DEVICE_SIZE / BLOCK_SIZE) == -1 {
                return Err(Error::last_os_error());
            }
            let (c, s) = socketpair()?;
            if libc::ioctl(fd, NBD_SET_SOCK, c.as_raw_fd() as u64) == -1 {
                return Err(Error::last_os_error());

            }
            let th = thread::spawn(move || libc::ioctl(fd, NBD_DO_IT));

            let mut v = Vec::with_capacity(DEVICE_SIZE as usize);
            v.set_len(DEVICE_SIZE as usize);
            Ok(NBD {
                   _nbd: f,
                   _client_sock: c,
                   server_sock: s,
                   mem: v,
                   th: th,
               })
        }
    }

    fn recv_req(&mut self) -> Result<nbd_request> {
        unsafe {
            let mut req = uninitialized::<nbd_request>();
            let p: *mut u8 = transmute(&mut req);
            let buf = slice::from_raw_parts_mut(p, size_of::<nbd_request>());
            self.server_sock.read_exact(buf).unwrap();
            // todo
            req.magic = u32::from_be(req.magic);
            req.type_ = u32::from_be(req.type_);
            req.from = u64::from_be(req.from);
            req.len = u32::from_be(req.len);
            assert!(req.magic == NBD_REQUEST_MAGIC);
            //print!("req:\n{:?}\n", req);
            Ok(req)
        }
    }

    fn process_read(&mut self, req: nbd_request) -> Result<()> {
        let begin = req.from as usize;
        let end = begin + req.len as usize;
        self.send_header(req)?;
        self.server_sock.write_all(&self.mem[begin..end])?;
        Ok(())
    }

    fn process_write(&mut self, req: nbd_request) -> Result<()> {
        let begin = req.from as usize;
        let end = begin + req.len as usize;
        self.server_sock.read_exact(&mut self.mem[begin..end])?;
        self.send_header(req)?;
        Ok(())
    }

    fn send_header(&mut self, req: nbd_request) -> Result<()> {
        self.server_sock.write_all(&NBD_REPLY_MAGIC)?;
        let error = [0u8; 4];
        self.server_sock.write_all(&error)?;
        self.server_sock.write_all(&req.handle)?;
        Ok(())
    }

    /// false to finish.
    fn process_req(&mut self, req: nbd_request) -> Result<bool> {
        match req.type_ {
            NBD_CMD_READ => self.process_read(req)?,
            NBD_CMD_WRITE => self.process_write(req)?,
            NBD_CMD_DISC => return Ok(false),
            NBD_CMD_FLUSH => self.send_header(req)?,
            NBD_CMD_TRIM => self.send_header(req)?,
            _ => panic!("unknown type: {}", req.type_),
        }
        Ok(true)
    }

    fn run(&mut self) {
        loop {
            let req = self.recv_req().unwrap();
            if !self.process_req(req).unwrap() {
                return;
            }
        }
    }
}

fn main() {
    let mut nbd = NBD::init().unwrap();
    print!("init\n");
    nbd.run();
}
